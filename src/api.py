from flask import Flask, request, jsonify
from flasgger import Swagger
from flask_cors import CORS
import json
import processing as p

app = Flask(__name__)

# allow cross-origin sharing to use http
CORS(app)

# adding Swagger
app.config['SWAGGER'] = {
    'title': 'Orders API',
    'uiversion': 3
}
Swagger(app, template_file='swagger.yaml')

@app.route('/api/load_table', methods=['GET'])
def load_table():
    """Load data from database"""
    table_name = request.args.get('table_name')
    engine = p.create_engine()
    result = p.load_data_from_db(table_name, engine)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/customers', methods=['GET'])
def customers_table():
    """Get customer spending analysis"""
    engine = p.create_engine()
    columns=['order_id', 'customer_unique_id', 'price', 'freight_value', 'payment_value', 'order_purchase_timestamp', 'customer_city']
    data = p.load_data_from_db('orders', engine, columns)
    result = p.get_customer_spending(data)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/sellers', methods=['GET'])
def sellers_table():
    """Get sales per seller analysis"""
    engine = p.create_engine()
    columns=['seller_id', 'order_id', 'price', 'item_quantity', 'seller_city']
    data = p.load_data_from_db('orders', engine, columns)
    result = p.get_sales_per_seller(data)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/products', methods=['GET'])
def product_table():
    """Get product sales analysis"""
    engine = p.create_engine()
    columns=['product_id', 'item_quantity', 'price', 'product_category_name_english', 'product_category_name']
    data = p.load_data_from_db('orders', engine, columns)
    result = p.get_product_sales_analysis(data)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/sales_analysis', methods=['GET'])
def sales_analysis():
    """Get overall sales analysis"""
    engine = p.create_engine()
    columns=['order_id', 'seller_id', 'customer_unique_id', 'product_id', 'price', 'freight_value']
    data = p.load_data_from_db('orders', engine, columns)
    result = p.get_sales_analysis(data)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/update_analytics', methods=['POST'])
def update_analytics():
    """Update analytics tables in the database"""
    engine = p.create_engine()
    p.analyze_and_load('orders', engine)
    return jsonify({'message': 'Analytics tables updated successfully'})

@app.route('/api/orders_by_date', methods=['GET'])
def orders_by_date():
    """Get orders within a specific date range (yyyy-mm-dd)"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    engine = p.create_engine()
    columns=['order_id', 'order_purchase_timestamp', 'order_delivered_customer_date']
    orders_df = p.load_data_from_db('orders', engine, columns)
    result = p.get_orders_by_date(orders_df, start_date, end_date)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/top_customers', methods=['GET'])
def top_customers():
    """Get top N customers by total spending"""
    n = int(request.args.get('n', 10))
    engine = p.create_engine()
    columns=['customer_unique_id', 'price', 'item_quantity']
    orders_df = p.load_data_from_db('orders', engine, columns)
    result = p.get_top_customers(orders_df, n)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/orders_by_customer', methods=['GET'])
def orders_by_customer():
    """Get orders for a specific customer"""
    customer_id = request.args.get('customer_id')
    engine = p.create_engine()
    columns=['order_id', 'customer_unique_id', 'price']
    orders_df = p.load_data_from_db('orders', engine, columns)
    result = p.get_orders_by_customer(orders_df, customer_id)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/orders_by_seller', methods=['GET'])
def orders_by_seller():
    """Get orders for a specific seller"""
    seller_id = request.args.get('seller_id')
    engine = p.create_engine()
    columns=['order_id', 'seller_id', 'price']
    orders_df = p.load_data_from_db('orders', engine, columns)
    result = p.get_orders_by_seller(orders_df, seller_id)
    return jsonify(json.loads(result.write_json()))

@app.route('/api/orders_by_product', methods=['GET'])
def orders_by_product():
    """Get orders for a specific product"""
    product_id = request.args.get('product_id')
    engine = p.create_engine()
    columns=['order_id', 'product_id', 'product_category_name','product_category_name_english']
    orders_df = p.load_data_from_db('orders', engine, columns)
    result = p.get_orders_by_product(orders_df, product_id)
    return jsonify(json.loads(result.write_json()))

if __name__ == '__main__':
    app.run(debug=True)

