import argparse
import logging
from src.database import create_database, create_schema
from src.etl import (
    load_grid_geometries, 
    load_provinces_geometries, 
    load_traffic_data, 
    load_mobility_data,
    get_top_cells
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_database():
    logger.info("=" * 60)
    logger.info("STEP 1: Database and schema creation")
    logger.info("=" * 60)
    create_database()
    create_schema()
    logger.info("✓ Database initialized\n")


def load_geometries():
    logger.info("=" * 60)
    logger.info("STEP 2: Geometries loading")
    logger.info("=" * 60)
    load_grid_geometries()
    load_provinces_geometries()
    logger.info("✓ Geometries loaded\n")


def load_csv_data(limit_files=None):
    logger.info("=" * 60)
    logger.info("STEP 3: CSV data loading")
    logger.info("=" * 60)
    load_traffic_data(limit_files=limit_files)
    load_mobility_data(limit_files=limit_files)
    logger.info("✓ CSV data loaded\n")


def run_test_query():
    logger.info("=" * 60)
    logger.info("STEP 4: Test query")
    logger.info("=" * 60)
    df = get_top_cells(limit=10)
    print("\nTop 10 cells by activity:")
    print(df.to_string(index=False))
    logger.info("✓ Query executed\n")


def main():
    parser = argparse.ArgumentParser(description='Milan Telecom ETL Pipeline')
    parser.add_argument('--setup', action='store_true', help='Create database and schema')
    parser.add_argument('--load-geo', action='store_true', help='Load geometries')
    parser.add_argument('--load-data', action='store_true', help='Load CSV data')
    parser.add_argument('--limit-files', type=int, default=None, help='Max CSV files to load')
    parser.add_argument('--test', action='store_true', help='Run test query')
    parser.add_argument('--all', action='store_true', help='Run all steps')
    
    args = parser.parse_args()
    
    try:
        if args.all:
            setup_database()
            load_geometries()
            load_csv_data(limit_files=args.limit_files)
            run_test_query()
            logger.info("=" * 60)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
        else:
            if args.setup:
                setup_database()
            
            if args.load_geo:
                load_geometries()
            
            if args.load_data:
                load_csv_data(limit_files=args.limit_files)
            
            if args.test:
                run_test_query()
            
            if not any([args.setup, args.load_geo, args.load_data, args.test]):
                parser.print_help()
                
    except Exception as e:
        logger.error(f"Execution error: {e}")
        raise


if __name__ == '__main__':
    main()
