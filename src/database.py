import os
import logging
from sqlalchemy import inspect, create_engine, Sequence, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import polars as pl
from datetime import datetime
from typing import Any, Type


# Set up logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the base for ORM models
Base: Type[Any] = declarative_base()

# ORM model for the 'orders' table
class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, Sequence('id'), primary_key=True)
    order_id = Column(String)
    customer_unique_id = Column(String)
    order_status = Column(String)
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)
    item_quantity = Column(Integer)
    price = Column(Float)
    freight_value = Column(Float)
    product_id = Column(String)
    product_category_name = Column(String)
    product_category_name_english = Column(String)
    customer_city = Column(String)
    seller_id = Column(String)
    seller_city = Column(String)
    payment_installments = Column(Integer)
    payment_value = Column(Float)
    review_score = Column(Integer)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)  


def setup_database(db_path: str) -> Any:
    """Sets up the DuckDB database and returns the engine."""
    logger.info("Setting up database...")
    engine = create_engine(f'duckdb:///{db_path}')
    
    # Drop existing table if exists
    inspector = inspect(engine)
    if 'orders' in inspector.get_table_names():
        # Drop the existing table
        Base.metadata.drop_all(tables=[Order.__table__], bind=engine)
        logger.info("Existing table dropped.")

    Base.metadata.create_all(engine)
    logger.info("Database setup complete.")
    return engine

def create_session(engine: Any) -> Session:
    """Creates and returns a new database session."""
    logger.info("Creating a new database session...")
    Session = sessionmaker(bind=engine)
    return Session()

def load_csv_data(working_dir: str) -> dict[str, pl.DataFrame]:
    """Load all necessary CSV files into Polars DataFrames."""
    logger.info("Loading CSV data into Polars DataFrames...")
    file_paths = {
        'customers': 'olist_customers_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
        'payments': 'olist_order_payments_dataset.csv',
        'reviews': 'olist_order_reviews_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'product_translation': 'product_category_name_translation.csv'
    }

    data_frames: dict[str, pl.DataFrame] = {}

    for key, filename in file_paths.items():
        full_path = os.path.join(working_dir, filename)
        if not os.path.exists(full_path):
            logger.error(f"File not found: {full_path}")
            raise FileNotFoundError(f"File not found: {full_path}")
        data_frames[key] = pl.read_csv(full_path)
        
    logger.info("Data loaded into DataFrames.")
    return data_frames

def process_data(data_frames: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Process data by joining, aggregating, and transforming."""
    
    logger.info("Processing data...")
    customers_df = data_frames['customers']
    orders_df = data_frames['orders']
    order_items_df = data_frames['order_items']
    payments_df = data_frames['payments']
    reviews_df = data_frames['reviews']
    products_df = data_frames['products']
    sellers_df = data_frames['sellers']
    product_translation_df = data_frames['product_translation']

    # Join orders with customers
    orders_with_customers = orders_df.join(customers_df, on='customer_id', how='left')

    # Join order items with products and product name in English
    order_items_with_products = order_items_df.join(products_df, on='product_id', how='left')

    # Aggregate order items to get total price and quantity
    order_items_agg = order_items_with_products.group_by('order_id').agg([
        pl.sum('price').alias('price'),
        pl.sum('freight_value').alias('freight_value'),
        pl.first('product_id'),
        pl.first('product_category_name'),
        pl.first('seller_id'),
        pl.count('order_item_id').alias('item_quantity')
    ])

    # Aggregate payments to get total payments for each order
    payments_agg = payments_df.group_by('order_id').agg([
        pl.first('payment_installments'),
        pl.sum('payment_value').alias('payment_value')
    ])

    # Get average review score by order_id
    reviews_agg = reviews_df.group_by('order_id').agg([
        pl.mean('review_score').cast(pl.Int32).alias('review_score')
    ])

    # Merge all DataFrames into a full orders DataFrame
    orders_full = (
        orders_with_customers
        .join(order_items_agg, on='order_id', how='left')
        .join(payments_agg, on='order_id', how='left')
        .join(sellers_df, on='seller_id', how='left')
        .join(reviews_agg, on='order_id', how='left')
        .join(product_translation_df, on='product_category_name', how='left')
    )

    # Select necessary columns for the final DataFrame
    final_orders_df = orders_full.select([
        'order_id',
        'customer_unique_id',
        'order_status',
        'order_purchase_timestamp',
        'order_approved_at',
        'order_delivered_carrier_date',
        'order_delivered_customer_date',
        'order_estimated_delivery_date',
        'customer_city',
        'price',
        'freight_value',
        'product_id',
        'product_category_name',
        'seller_id',
        'item_quantity',
        'payment_installments',
        'payment_value',
        'review_score',
        'seller_city',
        'product_category_name_english'
    ])

    logger.info("Orders DataFrame created.")
    #print(final_orders_df.describe())  # Log the description of the DataFrame

    return final_orders_df

def insert_data_into_db(session: Session, final_orders_df: pl.DataFrame) -> None:
    """Inserts processed data into the DuckDB database."""
    logger.info("Inserting data into the database...")
    
    try:
        records = final_orders_df.to_dicts()
        for record in records:
            order = Order(**record)
            session.add(order)
        session.commit()
        logger.info("Data successfully inserted into the database.")

    except Exception as e:
        logger.error(f"An error occurred during data insertion: {e}", exc_info=True)
        session.rollback()

    finally:
        session.close()
        logger.info("Database session closed.")

def data_pipeline(WORKING_DIR: str) -> None:
    ''' Runs the entire data pipeline'''
    logger.info("Starting data pipeline...")

    try:
        db_path = 'orders.db'
        data_frames = load_csv_data(WORKING_DIR)
        final_orders_df = process_data(data_frames)

        engine = setup_database(db_path)
        session = create_session(engine)
    
        insert_data_into_db(session, final_orders_df)
        logger.info("Data pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Data pipeline failed: {e}", exc_info=True)

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    WORKING_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))

    data_pipeline(WORKING_DIR)
    print('Data loaded to orders table success')

