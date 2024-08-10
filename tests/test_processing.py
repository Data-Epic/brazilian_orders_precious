import unittest
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

class TestProcessing(unittest.TestCase):

    def setUp(self):
        # Set database
        self.db_path = "test.db"
        self.engine = sa.create_engine(f'duckdb:///{self.db_path}')

        # Set up test data
        self.table_name = "test_table"
        self.data = pl.DataFrame({
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

    def tearDown(self):
        # Clean up test data
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_load_data_from_db(self):
        # Save the Polars DataFrame to a temporary CSV file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            self.data.write_csv(tmp_file.name)

        # Create a test DuckDB table
        with duckdb.connect(self.db_path, read_only=False) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM '{tmp_file.name}'")

        # Call the function
        result = load_data_from_db(self.table_name, self.engine)

        # Check the result
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, self.data.shape)

    def test_get_customer_spending(self):
        # Call the function
        result = get_customer_spending(self.data)

        # Check the result
        self.assertIsInstance(result, pl.DataFrame)
        self.assertIn('customer_unique_id', result.columns)

    def test_get_sales_per_seller(self):
        # Call the function
        result = get_sales_per_seller(self.data)

        # Check the result
        self.assertIsInstance(result, pl.DataFrame)
        self.assertIn('seller_id', result.columns)

    def test_get_product_sales_analysis(self):
        # Call the function
        result = get_product_sales_analysis(self.data)

        # Check the result
        self.assertIsInstance(result, pl.DataFrame)
        self.assertIn('product_id', result.columns)

    def test_get_sales_analysis(self):
        # Call the function
        result = get_sales_analysis(self.data)

        # Check the result
        self.assertIsInstance(result, pl.DataFrame)
        self.assertIn('top_seller_id', result.columns)

    def test_save_to_duckdb(self):
        # Call the function
        save_to_duckdb(self.data, self.table_name, self.engine)

        # Check if the table exists in DuckDB
        with duckdb.connect(self.db_path) as conn:
            result = conn.execute(f"SELECT * FROM {self.table_name}").df()

        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(self.data))

    def test_analyze_and_load(self):
        # Create and populate the test table first
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            self.data.write_csv(tmp_file.name)

        with duckdb.connect(self.db_path, read_only=False) as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} AS SELECT * FROM '{tmp_file.name}'")
        
        # Call the function
        analyze_and_load(self.table_name, self.engine)

        # Check if the tables exist in DuckDB
        with duckdb.connect(self.db_path) as conn:
            result1 = conn.execute("SELECT * FROM customers_analysis").df()
            result2 = conn.execute("SELECT * FROM sellers_analysis").df()
            result3 = conn.execute("SELECT * FROM products_analysis").df()
            result4 = conn.execute("SELECT * FROM sales_analysis").df()

        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
        self.assertIsNotNone(result3)
        self.assertIsNotNone(result4)
        self.assertGreater(len(result1), 0)
        self.assertGreater(len(result2), 0)
        self.assertGreater(len(result3), 0)
        self.assertGreater(len(result4), 0)

if __name__ == "__main__":
    unittest.main()
