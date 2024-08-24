import pytest
import polars as pl
import duckdb
import sqlalchemy as sa
import os
import tempfile
from src.processing import (
    load_data_from_db, 
    get_customer_spending, 
    get_sales_per_seller, 
    get_product_sales_analysis,
    get_sales_analysis, 
    save_to_duckdb, 
    analyze_and_load
)

@pytest.fixture(scope="function")
def setup_db():
    # Set up test database and engine
    db_path = "test.db"
    engine = sa.create_engine(f'duckdb:///{db_path}')
    
    yield db_path, engine
    
    # Clean up test data after each test
    if os.path.exists(db_path):
        os.remove(db_path)

@pytest.fixture
def test_data():
    return pl.DataFrame({
        "order_id": [1, 2, 3],
        "customer_unique_id": ["A", "B", "C"],
        "price": [10.0, 20.0, 30.0],
        "freight_value": [5.0, 10.0, 15.0],
        "payment_value": [15.0, 30.0, 45.0],
        "order_purchase_timestamp": ["2022-01-01", "2022-01-02", "2022-01-03"],
        "customer_city": ["City A", "City B", "City C"],
        "seller_id": ["S1", "S2", "S3"],
        "item_quantity": [1, 2, 3],
        "seller_city": ["City X", "City Y", "City Z"],
        "product_id": ["P1", "P2", "P3"],
        "product_category_name": ["Category 1", "Category 2", "Category 3"],
        "product_category_name_english": ["Category 1", "Category 2", "Category 3"]
    })

def test_load_data_from_db(setup_db, test_data):
    db_path, engine = setup_db
    table_name = "test_table"

    # Save the Polars DataFrame to a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        test_data.write_csv(tmp_file.name)

    # Create a test DuckDB table
    with duckdb.connect(db_path, read_only=False) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM '{tmp_file.name}'")

    # Call the function
    result = load_data_from_db(table_name, engine)

    # Check the result
    assert isinstance(result, pl.DataFrame)
    assert result.shape == test_data.shape

def test_get_customer_spending(test_data):
    # Call the function
    result = get_customer_spending(test_data)

    # Check the result
    assert isinstance(result, pl.DataFrame)
    assert 'customer_unique_id' in result.columns

def test_get_sales_per_seller(test_data):
    # Call the function
    result = get_sales_per_seller(test_data)

    # Check the result
    assert isinstance(result, pl.DataFrame)
    assert 'seller_id' in result.columns

def test_get_product_sales_analysis(test_data):
    # Call the function
    result = get_product_sales_analysis(test_data)

    # Check the result
    assert isinstance(result, pl.DataFrame)
    assert 'product_id' in result.columns

def test_get_sales_analysis(test_data):
    # Call the function
    result = get_sales_analysis(test_data)

    # Check the result
    assert isinstance(result, pl.DataFrame)
    assert 'top_seller_id' in result.columns

def test_save_to_duckdb(setup_db, test_data):
    db_path, engine = setup_db
    table_name = "test_table"

    # Call the function
    save_to_duckdb(test_data, table_name, engine)

    # Check if the table exists in DuckDB
    with duckdb.connect(db_path) as conn:
        result = conn.execute(f"SELECT * FROM {table_name}").df()

    assert result is not None
    assert len(result) == len(test_data)

def test_analyze_and_load(setup_db, test_data):
    db_path, engine = setup_db
    table_name = "test_table"

    # Save the Polars DataFrame to a temporary CSV file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        test_data.write_csv(tmp_file.name)

    # Create and populate the test table
    with duckdb.connect(db_path, read_only=False) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM '{tmp_file.name}'")
    
    # Call the function
    analyze_and_load(table_name, engine)

    # Check if the tables exist in DuckDB
    with duckdb.connect(db_path) as conn:
        result1 = conn.execute("SELECT * FROM customers_analysis").df()
        result2 = conn.execute("SELECT * FROM sellers_analysis").df()
        result3 = conn.execute("SELECT * FROM products_analysis").df()
        result4 = conn.execute("SELECT * FROM sales_analysis").df()

    assert result1 is not None
    assert result2 is not None
    assert result3 is not None
    assert result4 is not None
    assert len(result1) > 0
    assert len(result2) > 0
    assert len(result3) > 0
    assert len(result4) > 0
