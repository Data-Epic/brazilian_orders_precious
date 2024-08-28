from flask import Flask, request, jsonify
from flasgger import Swagger
from flask_cors import CORS
import json
from . import processing as p

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
    if not table_name:
        return jsonify({'error': 'Table name is required'}), 400
    try:
        engine = p.create_engine()
        result = p.load_data_from_db(table_name, engine)
        return jsonify({"status": "success", "message": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/customers', methods=['GET'])
def customers_table():
    """Get customer spending analysis"""
    try:
        engine = p.create_engine()
        columns = ['order_id', 'customer_unique_id', 'price', 'freight_value', 'payment_value', 'order_purchase_timestamp', 'customer_city']
        data = p.load_data_from_db('orders', engine, columns)
        result = p.get_customer_spending(data)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/sellers', methods=['GET'])
def sellers_table():
    """Get sales per seller analysis"""
    try:
        engine = p.create_engine()
        columns = ['seller_id', 'order_id', 'price', 'item_quantity', 'seller_city']
        data = p.load_data_from_db('orders', engine, columns)
        result = p.get_sales_per_seller(data)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/products', methods=['GET'])
def product_table():
    """Get product sales analysis"""
    try:
        engine = p.create_engine()
        columns = ['product_id', 'item_quantity', 'price', 'product_category_name_english', 'product_category_name']
        data = p.load_data_from_db('orders', engine, columns)
        result = p.get_product_sales_analysis(data)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/sales_analysis', methods=['GET'])
def sales_analysis():
    """Get overall sales analysis"""
    try:
        engine = p.create_engine()
        columns = ['order_id', 'seller_id', 'customer_unique_id', 'product_id', 'price', 'freight_value']
        data = p.load_data_from_db('orders', engine, columns)
        result = p.get_sales_analysis(data)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_analytics', methods=['POST'])
def update_analytics():
    """Update analytics tables in the database"""
    try:
        engine = p.create_engine()
        p.analyze_and_load('orders', engine)
        return jsonify({"status": "success", "message": "Analytics tables updated successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/orders_by_date', methods=['GET'])
def orders_by_date():
    """Get orders within a specific date range (yyyy-mm-dd)"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"status": "error", "message": "Start date and end date are required"}), 400
    try:
        engine = p.create_engine()
        columns = ['order_id', 'order_purchase_timestamp', 'order_delivered_customer_date']
        orders_df = p.load_data_from_db('orders', engine, columns)
        result = p.get_orders_by_date(orders_df, start_date, end_date)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/top_customers', methods=['GET'])
def top_customers():
    """Get top N customers by total spending"""
    try:
        n = int(request.args.get('n', 10))
        if n <= 0:
            return jsonify({"status": "error", "message": "Number of top customers must be positive"}), 400
        engine = p.create_engine()
        columns = ['customer_unique_id', 'price', 'item_quantity']
        orders_df = p.load_data_from_db('orders', engine, columns)
        result = p.get_top_customers(orders_df, n)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/orders_by_customer', methods=['GET'])
def orders_by_customer():
    """Get orders for a specific customer"""
    customer_id = request.args.get('customer_id')
    if not customer_id:
        return jsonify({"status": "error", "message": "Customer ID is required"}), 400
    try:
        engine = p.create_engine()
        columns = ['order_id', 'customer_unique_id', 'price']
        orders_df = p.load_data_from_db('orders', engine, columns)
        result = p.get_orders_by_customer(orders_df, customer_id)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/orders_by_seller', methods=['GET'])
def orders_by_seller():
    """Get orders for a specific seller"""
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return jsonify({"status": "error", "message": "Seller ID is required"}), 400
    try:
        engine = p.create_engine()
        columns = ['order_id', 'seller_id', 'price']
        orders_df = p.load_data_from_db('orders', engine, columns)
        result = p.get_orders_by_seller(orders_df, seller_id)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/orders_by_product', methods=['GET'])
def orders_by_product():
    """Get orders for a specific product"""
    product_id = request.args.get('product_id')
    if not product_id:
        return jsonify({"status": "error", "message": "Product ID is required"}), 400
    try:
        engine = p.create_engine()
        columns = ['order_id', 'product_id', 'product_category_name', 'product_category_name_english']
        orders_df = p.load_data_from_db('orders', engine, columns)
        result = p.get_orders_by_product(orders_df, product_id)
        return jsonify({"status": "success", "data": json.loads(result.write_json())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

