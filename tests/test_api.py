import unittest
from src.api import app

class TestAPI(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app_context = app.app_context()
        self.app_context.push()

    def tearDown(self):
        self.app_context.pop()

    def test_load_table(self):
        response = self.app.get('/api/load_table?table_name=orders')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_customers_table(self):
        response = self.app.get('/api/customers')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_sellers_table(self):
        response = self.app.get('/api/sellers')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_product_table(self):
        response = self.app.get('/api/products')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_sales_analysis(self):
        response = self.app.get('/api/sales_analysis')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a dictionary
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_update_analytics(self):
        response = self.app.post('/api/update_analytics')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {'message': 'Analytics tables updated successfully'})

    def test_orders_by_date(self):
        response = self.app.get('/api/orders_by_date?start_date=2020-01-01&end_date=2020-01-31')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list

    def test_top_customers(self):
        response = self.app.get('/api/top_customers?n=5')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list
        self.assertGreater(len(response.json), 0)  # Check if response is not empty

    def test_orders_by_customer(self):
        response = self.app.get('/api/orders_by_customer?customer_id=123')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list

    def test_orders_by_seller(self):
        response = self.app.get('/api/orders_by_seller?seller_id=456')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list

    def test_orders_by_product(self):
        response = self.app.get('/api/orders_by_product?product_id=789')
        self.assertEqual(response.status_code, 200)
        self.assertIsNotNone(response.json)
        self.assertIsInstance(response.json, list)  # Check if response is a list

    # @patch('src.processing.create_engine')
    # def test_create_engine_called(self, mock_create_engine):
    #     self.app.get('/api/load_table?table_name=orders')
    #     mock_create_engine.assert_called_once()  # Check if create_engine is called
    
if __name__ == '__main__':
    unittest.main()