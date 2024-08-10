# Brazilian Customers Orders Analytics Pipeline

This project implements an ETL pipeline for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion and data processing scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability.The Docker environment is designed to extract and process the data, and provide a command-line interface (CLI) for querying the data using DuckDB.

## Features

- Data extraction from multiple CSV files in the Olist dataset
- Data transformation and modeling to create order table using SQLALchemy ORM
- Data Loading into 
- Advanced data analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- Comprehensive unit tests for data preprocessing and database operations
- Full dockerization of the ETL pipeline for reproducibility and portability

## Prerequisites

- Docker and Docker Compose
- Git

## Quick Start

#### 1. Clone the repository:
```bash
git clone https://github.com/Data-Epic/brazilian_orders_precious.git
cd customer_orders_analysis
```

#### 2. Download the dataset:
- Get the list dataset and information from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), unzip the downloaded zip file
- Place all CSV files in the `data/` directory

#### 3. Build the Docker image:
```bash
docker-compose build
```

This will:
- Build the Docker image based on the provided Dockerfile.
- Install necessary dependencies.
- Download and unzip the DuckDB CLI.
- Run the database and processing scripts.
- Create the `orders-analysis` image.

#### 4. Run the container:
```bash
docker run -it orders-analysis
```
This opens the container shell and allows you to run queries. 
Aliases has been created for tasks like running tests and opening the `orders.db` in DuckDB CLI
- `runtests`: to run tests
- `rundb` to open orders database in DuckDB CLi
- `runpipeline` to run the database and processing scripts

**Example usage**
```
root@4608a104ade6:/orders_analysis# runtests
```

#### 5. Query the loaded tables in DuckDB:
```sql
SELECT * FROM orders LIMIT 5;
SELECT * FROM customers_analysis  LIMIT 5;
SELECT * FROM sales_analysis;
```

## Project Info

### Input Data
```
- olist_customers_dataset.csv
- olist_order_items_dataset.csv
- olist_orders_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv
- olist_order_payments_dataset.csv
```

### Output Tables
```
1. orders - Contains joined data from the csv files including order_id, customer_unique_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, item_quantity, price, freight_value, product_id, product_category_name, product_category_name_english, customer_city, seller_id, seller_city, payment_installments, payment_value, and review_score.

2. customers_analysis - Contains data related to customer spending, including customer_unique_id, total_orders_value, order_count, total_shipping_cost, total_payment_value, last_order_date, first_order_date, and customer_city.

3. sellers_analysis - Contains data on sales per seller, including seller_id, total_orders_value, total_orders, total_products_sold, and seller_city.

4. products_analysis - Contains product sales analysis data, including product_id, total_sold, total_sales, average_price, and product_category.

5. sales_analysis - Contains overall sales analysis data, including top_seller_id, top_seller_sales, top_customer_id, top_customer_spent, most_sold_product_id, most_sold_product_count, avg_order_value, and avg_shipping_fee.
```

### Project Structure
```
customer_orders_analysis/
├── data/                  # Input CSV files
├── src/                   # Source code files
|── tests/                 # Unit tests
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

## Customization

#### Updating Dependencies:

If you need to update or add dependencies and packages, modify the requirements.txt file and rebuild the Docker image using: 
``` bash
docker-compose up --build
```

#### Data Ingestion and Processing:

Modify the database.py and/or processing.py scripts to change the data ingestion/processing processes according to your requirements. Also, update the main.py script and the test scripts as needed.

#### DuckDB installation failed

If the build fails due to the duckdb installation, download the duckdb binary file on your local machine and move it into the 
project directory. <br>
Copy it into the container and rebuild your image. Do this by editing Dockerfile with the code below.
```Dockerfile
  # Download and install DuckDB CLI
  #RUN apt-get update && apt-get install -y wget unzip
  #RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
  #RUN unzip duckdb_cli-linux-amd64.zip
  #RUN rm duckdb_cli-linux-amd64.zip
  COPY ./duckdb ./duckdb
  RUN chmod +x ./duckdb
```

#### Troubleshooting:

If you encounter issues, ensure that all dependencies are correctly installed and that the CSV files are properly placed in the data/ directory. Common issues include file not found errors or Docker build problems.



