import sys
import logging
from scripts import etl, load_db

# Logging settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_log.txt'),
        logging.StreamHandler()
    ]
)

def main():
    try:
        logging.info("Launching ETL pipeline...")
        etl.main()  
        logging.info("ETL pipeline completed successfully")
        
        logging.info("Launching SQL scripts execution...")
        load_db.main()  
        logging.info("SQL scripts execution completed successfully")
        
        logging.info("Full pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()