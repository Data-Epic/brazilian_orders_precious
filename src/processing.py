import polars as pl
import logging
import sqlalchemy as sa
from .database import data_pipeline
from datetime import datetime
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set database path to the parent directory
db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'orders.db')


def create_engine():
    """Create database connection. If the database doesn't exist, create it and populate it from database.py"""
    #create and populate the database if it doesn't exist
    if not os.path.exists(db_path):
        logging.info("Database doesn't exist. Creating database....")

        # Set working directory and database path
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        WORKING_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))

        data_pipeline(WORKING_DIR)

    engine = sa.create_engine(f'duckdb:///{db_path}')
    return engine

def load_data_from_db(table_name: str, engine, columns: list[str] = None, where: str = None) -> pl.DataFrame:
    """Load data from a DuckDB table into a Polars DataFrame."""
    logging.info(f"Loading data from {table_name}...")

    try:
        # Reflect the table from the database
        metadata = sa.MetaData()
        table = sa.Table(table_name, metadata, autoload_with=engine)
        
        # If columns are provided, convert them into Column objects
        if columns:
            columns_to_select = [getattr(table.c, col) for col in columns]
        else:
            # Select all columns if none are specified
            columns_to_select = [table.c]

        # Create the select statement
        stmt = sa.select(*columns_to_select)
        
        # Add optional where clause
        if where:
            stmt = stmt.where(where)
        
        # Execute the SQL expression using the engine
        with engine.connect() as connection:
            result = connection.execute(stmt).fetchall()
            data = [dict(row._mapping) for row in result]
            df = pl.DataFrame(data)

        logging.info(f"Successfully loaded data from {table_name}")
        return df
    
    except Exception as e:
        logging.error(f"Failed to load data from {table_name}: {e}")
        raise

def get_customer_spending(data):
    logging.info("Calculating customer spending...")
    customers_df = data.group_by('customer_unique_id').agg([
        pl.sum('price').round(2).alias('total_orders_value'),
        pl.count('order_id').alias('order_count'),
        pl.sum('freight_value').round(2).alias('total_shipping_cost'),
        pl.sum('payment_value').round(2).alias('total_payment_value'),
        pl.max('order_purchase_timestamp').alias('last_order_date'),
        pl.min('order_purchase_timestamp').alias('first_order_date'),
        pl.first('customer_city')
    ])
    logging.info("Customer spending calculation completed.")
    return customers_df

def get_sales_per_seller(data):
    logging.info("Calculating sales per seller...")
    sellers_df = data.group_by('seller_id').agg([
        pl.sum('price').round(2).alias('total_orders_value'),
        pl.count('order_id').alias('total_orders'),
        pl.sum('item_quantity').alias('total_products_sold'),
        pl.first('seller_city')
    ])
    logging.info("Sales per seller calculation completed.")
    return sellers_df

def get_product_sales_analysis(data):
    logging.info("Performing product sales analysis...")
    # Add product category name in English and if null, add in Portuguese in product df
    products_df = data.with_columns([
        pl.when(pl.col('product_category_name_english').is_null())
        .then(pl.col('product_category_name'))
        .otherwise(pl.col('product_category_name_english'))
        .alias('product_category_name')
    ])
    
    products_df = data.group_by('product_id').agg([
        pl.sum('item_quantity').alias('total_sold'),
        pl.sum('price').round(2).alias('total_sales'),
        pl.mean('price').round(2).alias('average_price'),
        pl.first('product_category_name_english').alias('product_category')
    ]).sort('total_sold', descending=True)

    logging.info("Product sales analysis completed.")
    return products_df

def get_sales_analysis(data):
    logging.info("Running overall sales analysis...")
    # Top Seller
    top_seller = data.group_by('seller_id').agg([
        pl.sum('price').round(2).alias('total_sales')
    ]).sort('total_sales', descending=True)[0]
    top_seller_id = top_seller['seller_id']  # Extract the seller_id value
    top_seller_sales = top_seller['total_sales']  # Extract and round the total_sales value


    # Top Customer
    top_customer = data.group_by('customer_unique_id').agg([
        pl.sum('price').round(2).alias('total_spent')
    ]).sort('total_spent', descending=True)[0]
    top_customer_id = top_customer['customer_unique_id']  # Extract the customer_unique_id value
    top_customer_spent = top_customer['total_spent']  # Extract the total_spent value

    # Most Sold Product
    most_sold_product = data.group_by('product_id').agg([
        pl.count('order_id').alias('product_sales_count')
    ]).sort('product_sales_count', descending=True)[0]
    most_sold_product_id = most_sold_product['product_id']  # Extract the product_id value
    most_sold_product_count = most_sold_product['product_sales_count']  # Extract the product_sales_count value

    # Average Price per Order
    avg_price_per_order = data.select(pl.mean('price').round(2).alias('avg_order_value'))

    # Average Shipping Fee
    avg_shipping_fee = data.select(pl.mean('freight_value').round(2).alias('avg_shipping_fee'))

    # Combine into a single DataFrame
    sales_analysis_df = pl.DataFrame({
        "top_seller_id": top_seller_id,
        "top_seller_sales": top_seller_sales,
        "top_customer_id": top_customer_id,
        "top_customer_spent": top_customer_spent,
        "most_sold_product_id": most_sold_product_id,
        "most_sold_product_count": most_sold_product_count,
        "avg_order_value": avg_price_per_order['avg_order_value'],
        "avg_shipping_fee": avg_shipping_fee['avg_shipping_fee']
    })

    logging.info("Overall sales analysis completed.")
    return sales_analysis_df

def save_to_duckdb(data: pl.DataFrame, table_name: str, engine):
    """Save a Polars DataFrame to a DuckDB table."""
    try:
        logging.info(f"Saving DataFrame to table: {table_name}")
        data.write_database(table_name=table_name, connection=engine, if_table_exists='replace')
        logging.info(f"DataFrame successfully saved to {table_name}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to {table_name}: {e}")
        raise

def analyze_and_load(table_name: str, engine):
    """Runs the full analytics pipeline on the orders database and save to duckdb."""
    logging.info("Starting analysis and loading data into DuckDB.")
    
    orders_df = load_data_from_db(table_name, engine)
    
    dfs = {
        'customers_analysis': get_customer_spending(orders_df),
        'sellers_analysis': get_sales_per_seller(orders_df),
        'products_analysis': get_product_sales_analysis(orders_df),
        'sales_analysis': get_sales_analysis(orders_df)
    }

    for table, df in dfs.items():
        save_to_duckdb(df, table, engine)

    logging.info("Analysis and data loading completed.")

# Simple filtering functions

def get_orders_by_date(data, start_date, end_date):
    """Get orders within a specific date range e.g 2017-08-10 - 2018-10-12"""
    logging.info(f"Getting orders between {start_date} and {end_date}")
    
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    filtered_orders_df = data.filter((pl.col('order_purchase_timestamp') >= start_date) & (pl.col('order_purchase_timestamp') <= end_date))
    return filtered_orders_df

def get_top_customers(data, n=10):
    """Get top N customers by total spending"""
    logging.info(f"Getting top {n} customers by total spending")
    customers_df = data.group_by('customer_unique_id').agg([
        pl.sum('price').round(2).alias('total_orders_value'),
        pl.count('item_quantity').alias('total_items_ordered')
        ]).sort('total_orders_value', descending=True).head(n)
    return customers_df

def get_orders_by_customer(data, customer_id):
    """Get orders for a specific customer"""
    logging.info(f"Getting orders for customer {customer_id}")
    filtered_orders_df = data.filter(pl.col('customer_unique_id') == customer_id)
    return filtered_orders_df

def get_orders_by_seller(data, seller_id):
    """Get orders for a specific seller"""
    logging.info(f"Getting orders for seller {seller_id}")
    filtered_orders_df = data.filter(pl.col('seller_id') == seller_id)
    return filtered_orders_df

def get_orders_by_product(data, product_id):
    """Get orders for a specific product"""
    logging.info(f"Getting orders for product {product_id}")
    filtered_orders_df = data.filter(pl.col('product_id') == product_id)
    return filtered_orders_df



if __name__ == "__main__":
    # Perform analysis and load data
    engine = create_engine()
    analyze_and_load('orders', engine)

