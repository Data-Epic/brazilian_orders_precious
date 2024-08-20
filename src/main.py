import os
import logging
from database import data_pipeline, 
from processing import analyze_and_load, create_engine

def main():
    # Set up logging
    
    logger = logging.getLogger(__name__)
    
    # Set working directory and database path
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    WORKING_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '../data'))
    
    try:
        # Run data pipeline from database.py
        logger.info("Starting data pipeline...")
        data_pipeline(WORKING_DIR)
        logger.info("Data pipeline completed successfully.")
        
        # Run analysis from processing.py
        logger.info("Starting data analysis...")
        engine = create_engine()
        analyze_and_load('orders', engine)
        logger.info("Data analysis completed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()