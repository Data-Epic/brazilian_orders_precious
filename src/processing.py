import polars as pl
import duckdb
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_db(db_path: str, table_name: str) -> pl.DataFrame:
    """Load data from a DuckDB table into a Polars DataFrame."""
    try:
        query = f"SELECT * FROM {table_name}"
        logging.info(f"Loading data from table: {table_name}")
        with duckdb.connect(db_path) as conn:
            df = conn.execute(query).pl()
        logging.info(f"Successfully loaded data from {table_name}")
        return df
    except Exception as e:
        logging.error(f"Failed to load data from {table_name}: {e}")
        raise

def get_customer_spending(data):
    logging.info("Calculating customer spending...")
    customers_df = data.group_by('customer_unique_id').agg([
        pl.sum('price').alias('total_orders_value'),
        pl.count('order_id').alias('order_count'),
        pl.sum('freight_value').alias('total_shipping_cost'),
        pl.sum('payment_value').alias('total_payment_value'),
        pl.max('order_purchase_timestamp').alias('last_order_date'),
        pl.min('order_purchase_timestamp').alias('first_order_date'),
        pl.first('customer_city')
    ])
    logging.info("Customer spending calculation completed.")
    return customers_df

def get_sales_per_seller(data):
    logging.info("Calculating sales per seller...")
    sellers_df = data.group_by('seller_id').agg([
        pl.sum('price').alias('total_orders_value'),
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
        pl.sum('price').alias('total_sales'),
        pl.mean('price').alias('average_price'),
        pl.first('product_category_name_english').alias('product_category')
    ]).sort('total_sold', descending=True)

    logging.info("Product sales analysis completed.")
    return products_df

def get_sales_analysis(data):
    logging.info("Running overall sales analysis...")
    # Top Seller
    top_seller = data.group_by('seller_id').agg([
        pl.sum('price').alias('total_sales')
    ]).sort('total_sales', descending=True)[0]

    # Top Customer
    top_customer = data.group_by('customer_unique_id').agg([
        pl.sum('price').alias('total_spent')
    ]).sort('total_spent', descending=True)[0]

    # Most Sold Product
    most_sold_product = data.group_by('product_id').agg([
        pl.count('order_id').alias('product_sales_count')
    ]).sort('product_sales_count', descending=True)[0]

    # Average Price per Order
    avg_price_per_order = data.select(pl.mean('price').alias('avg_order_value'))

    # Average Shipping Fee
    avg_shipping_fee = data.select(pl.mean('freight_value').alias('avg_shipping_fee'))

    # Combine into a single DataFrame
    sales_analysis_df = pl.DataFrame({
        "top_seller_id": [top_seller['seller_id']],
        "top_seller_sales": [top_seller['total_sales']],
        "top_customer_id": [top_customer['customer_unique_id']],
        "top_customer_spent": [top_customer['total_spent']],
        "most_sold_product_id": [most_sold_product['product_id']],
        "most_sold_product_count": [most_sold_product['product_sales_count']],
        "avg_order_value": avg_price_per_order['avg_order_value'],
        "avg_shipping_fee": avg_shipping_fee['avg_shipping_fee']
    })

    logging.info("Overall sales analysis completed.")
    return sales_analysis_df

def save_to_duckdb(db_path: str, data: pl.DataFrame, table_name: str):
    """Save a Polars DataFrame to a DuckDB table."""
    try:
        logging.info(f"Saving DataFrame to table: {table_name}")
        with duckdb.connect(db_path, read_only=False) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM data")
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM data")
        logging.info(f"DataFrame successfully saved to {table_name}")
    except Exception as e:
        logging.error(f"Failed to save DataFrame to {table_name}: {e}")
        raise

def analyze_and_load(db_path: str, table_name: str):
    """Runs the full analytics pipeline on the orders database and save to duckdb."""
    logging.info("Starting analysis and loading data into DuckDB.")
    
    orders_df = load_data_from_db(db_path, table_name)
    
    customers_df = get_customer_spending(orders_df)
    save_to_duckdb(db_path, customers_df, 'customers_analysis')
    
    sellers_df = get_sales_per_seller(orders_df)
    save_to_duckdb(db_path, sellers_df, 'sellers_analysis')
    
    products_df = get_product_sales_analysis(orders_df)
    save_to_duckdb(db_path, products_df, 'products_analysis')
    
    sales_analysis_df = get_sales_analysis(orders_df)
    save_to_duckdb(db_path, sales_analysis_df, 'sales_analysis')

    logging.info("Analysis and data loading completed.")

if __name__ == "__main__":
    # Set working directory and database path
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    WORKING_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
    
    # Set database path
    db_path = os.path.join(WORKING_DIR, 'orders.db')
    analyze_and_load(db_path, 'orders')
