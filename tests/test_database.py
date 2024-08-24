import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import setup_database, create_session, insert_data_into_db, process_data, Order
import polars as pl
from datetime import datetime
import os

@pytest.fixture(scope='function')
def setup_db():
    """Set up the test database and session."""
    db_path = 'test_orders.db'
    engine = setup_database(db_path)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield engine, session
    
    # Teardown after the test
    if os.path.exists(db_path):
        os.remove(db_path)
    session.close()
    engine.dispose()

def create_mock_data():
    """Create detailed mock data for testing."""
    # Mock data for customers
    customers_df = pl.DataFrame({
        'customer_id': ['1', '2'],
        'customer_unique_id': ['unique_1', 'unique_2'],
        'customer_city': ['city_1', 'city_2']
    })

    # Mock data for orders with correct datetime format
    orders_df = pl.DataFrame({
        'order_id': ['order_1', 'order_2'],
        'customer_id': ['1', '2'],
        'order_status': ['delivered', 'shipped'],
        'order_purchase_timestamp': [datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 2, 11, 0, 0)],
        'order_approved_at': [datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 2, 13, 0, 0)],
        'order_delivered_carrier_date': [datetime(2024, 1, 3, 14, 0, 0), datetime(2024, 1, 4, 15, 0, 0)],
        'order_delivered_customer_date': [datetime(2024, 1, 5, 16, 0, 0), datetime(2024, 1, 6, 17, 0, 0)],
        'order_estimated_delivery_date': [datetime(2024, 1, 7, 18, 0, 0), datetime(2024, 1, 8, 19, 0, 0)]
    })

    # Convert datetime values to strings
    orders_df = orders_df.with_columns([
        pl.col('order_purchase_timestamp').cast(pl.Datetime),
        pl.col('order_approved_at').cast(pl.Datetime),
        pl.col('order_delivered_carrier_date').cast(pl.Datetime),
        pl.col('order_delivered_customer_date').cast(pl.Datetime),
        pl.col('order_estimated_delivery_date').cast(pl.Datetime)
    ])

    # Mock data for order items
    order_items_df = pl.DataFrame({
        'order_id': ['order_1', 'order_1', 'order_2'],
        'order_item_id': [1, 2, 1],
        'seller_id': ['seller_1', 'seller_2', 'seller_2'],
        'price': [50.0, 30.0, 20.0],
        'freight_value': [5.0, 5.0, 5.0],
        'product_id': ['product_1', 'product_2', 'product_3']
    })

    # Mock data for payments
    payments_df = pl.DataFrame({
        'order_id': ['order_1', 'order_2'],
        'payment_installments': [1, 2],
        'payment_value': [85.0, 25.0]
    })

    # Mock data for reviews
    reviews_df = pl.DataFrame({
        'order_id': ['order_1', 'order_2'],
        'review_score': [5, 4]
    })

    # Mock data for products
    products_df = pl.DataFrame({
        'product_id': ['product_1', 'product_2', 'product_3'],
        'product_category_name': ['category_1', 'category_2', 'category_3'],
        'product_category_name_english': ['English Category 1', 'English Category 2', 'English Category 3']
    })

    # Mock data for sellers
    sellers_df = pl.DataFrame({
        'seller_id': ['seller_1', 'seller_2'],
        'seller_city': ['seller_city_1', 'seller_city_2']
    })

    # Mock data for product translations
    product_translation_df = pl.DataFrame({
        'product_category_name': ['category_1', 'category_2'],
        'product_category_name_english': ['English Category 1', 'English Category 2']
    })

    return {
        'customers': customers_df,
        'orders': orders_df,
        'order_items': order_items_df,
        'payments': payments_df,
        'reviews': reviews_df,
        'products': products_df,
        'sellers': sellers_df,
        'product_translation': product_translation_df
    }

def test_setup_database(setup_db):
    """Test the setup_database function."""
    engine, _ = setup_db
    assert engine is not None

def test_create_session(setup_db):
    """Test the create_session function."""
    engine, _ = setup_db
    session = create_session(engine)
    assert session is not None

def test_insert_data_into_db(setup_db):
    """Test the insert_data_into_db function."""
    _, session = setup_db
    data_frames = create_mock_data()
    final_orders_df = process_data(data_frames)
    insert_data_into_db(session, final_orders_df)

    # Verify insertion
    result = session.query(Order).count()
    assert result == 2  # Check if 2 rows are inserted

def test_process_data():
    """Test the process_data function."""
    data_frames = create_mock_data()
    final_orders_df = process_data(data_frames)

    assert final_orders_df.shape[0] == 2  # Check number of rows
    assert 'order_id' in final_orders_df.columns  # Check if 'order_id' is present
