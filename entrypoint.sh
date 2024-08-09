#!/bin/sh

# Ensure the orders.db file exists (created by the app)
while [ ! -f /orders_analysis/data/orders.db ]; do
  echo "Waiting for orders.db to be created..."
  sleep 60 # Check every 1 minutes
done

# Open DuckDB CLI with orders.db
/orders_analysis/duckdb /orders_analysis/data/orders.db

tail -f /dev/null