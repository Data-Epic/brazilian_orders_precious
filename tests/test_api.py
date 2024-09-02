import pytest
from unittest.mock import patch, MagicMock
import polars as pl
from src.api import app as flask_app

# Updated Mock Orders DataFrame with 'order_delivered' column
mock_orders_df = pl.DataFrame({
    'order_id': [1, 2],
    'customer_unique_id': ['cust_1', 'cust_2'],
    'price': [100, 200],
    'freight_value': [10, 20],
    'payment_value': [110, 220],
    'customer_city': ['city_1', 'city_2'],
    'seller_id': ['seller_1', 'seller_2'],
    'seller_city': ['city_1', 'city_2'],
    'item_quantity': [1, 2],
    'product_id': ['prod_1', 'prod_2'],
    'product_category_name': ['category_1', 'category_2'],
    'product_category_name_english': ['category_1_en', 'category_2_en'],
    'order_purchase_timestamp': ['2022-01-01', '2022-01-02'],
    'order_delivered_customer_date': ['2022-01-05', '2022-01-06']  
})

#convert dates to datetime
mock_orders_df = mock_orders_df.with_columns(
    pl.col('order_purchase_timestamp').str.strptime(pl.Date, format='%Y-%m-%d'),
    pl.col('order_delivered_customer_date').str.strptime(pl.Date, format='%Y-%m-%d')
)

# Mock "database" function to return the right DataFrame
def mock_load_data_from_db(table_name, engine, columns=None):
    
    if columns:
        return mock_orders_df.select(columns)
    return mock_orders_df

@pytest.fixture
def client():
    with flask_app.test_client() as client:
        yield client

@pytest.fixture(autouse=True)
def reset_mock_df():
    global mock_orders_df
    mock_orders_df = mock_orders_df.sort("order_id")  # Reset mock DF between tests
    yield

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_load_table(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/load_table?table_name=orders')
    assert response.status_code == 200
    response_json = response.get_json()
    assert response_json['status'] == 'success'
    assert len(response_json['message']) > 0

@patch('src.api.p.create_engine')  
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db) 
def test_customers_table(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/customers')
    assert response.status_code == 200
    response_json = response.get_json()
    assert response_json['status'] == 'success'
    assert len(response_json['message']) == 2    
    assert {customer['customer_unique_id'] for customer in response_json['message']} == {'cust_1', 'cust_2'}

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_sellers_table(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/sellers')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 2
    assert {seller['seller_id'] for seller in response_json['message']} == {'seller_1', 'seller_2'}

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_product_table(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/products')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 2
    assert {product['product_id'] for product in response_json['message']} == {'prod_1', 'prod_2'}

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_sales_analysis(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/sales_analysis')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 1
    assert response_json['message'][0]['avg_order_value'] == 150.0
    assert response_json['message'][0]['avg_shipping_fee'] == 15.0
    assert response_json['message'][0]['most_sold_product_count'] == 1


@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_orders_by_date(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/orders_by_date?start_date=2022-01-01&end_date=2022-01-02')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 2
    assert {order['order_purchase_timestamp'] for order in response_json['message']} == {'2022-01-01', '2022-01-02'}

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_top_customers(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/top_customers?n=2')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 2
    assert {customer['customer_unique_id'] for customer in response_json['message']} == {'cust_1', 'cust_2'}

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_orders_by_customer(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/orders_by_customer?customer_id=cust_1')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 1
    assert response_json['message'][0]['customer_unique_id'] == 'cust_1'

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_orders_by_seller(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/orders_by_seller?seller_id=seller_1')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 1
    assert response_json['message'][0]['seller_id'] == 'seller_1'

@patch('src.api.p.create_engine')
@patch('src.api.p.load_data_from_db', side_effect=mock_load_data_from_db)
def test_orders_by_product(mock_load_data_from_db, mock_create_engine, client):
    mock_create_engine.return_value = MagicMock()
    response = client.get('/api/orders_by_product?product_id=prod_1')
    assert response.status_code == 200
    response_json = response.get_json()
    assert len(response_json['message']) == 1
    assert response_json['message'][0]['product_id'] == 'prod_1'

