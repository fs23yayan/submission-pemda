"""
Main ETL Pipeline Orchestrator
Menjalankan seluruh proses Extract, Transform, dan Load
"""

import logging
import sys
from datetime import datetime
from utils.extract import scrape_all_pages
from utils.transform import transform_data
from utils.load import load_data_to_all_repositories

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """
    Fungsi utama untuk menjalankan seluruh ETL pipeline
    
    Proses:
    1. Extract: Scrape data dari website Fashion Studio
    2. Transform: Clean dan transform data
    3. Load: Simpan ke CSV, Google Sheets, dan PostgreSQL
    
    Returns:
        dict: Status hasil pipeline
    """
    try:
        logger.info("="*70)
        logger.info("ETL PIPELINE STARTED")
        logger.info("="*70)
        start_time = datetime.now()
        
        # ============================================================
        # STEP 1: EXTRACT
        # ============================================================
        logger.info("\n" + "="*70)
        logger.info("STEP 1: EXTRACT - Scraping data from website")
        logger.info("="*70)
        
        try:
            # Scrape all 50 pages
            df_raw = scrape_all_pages(
                base_url="https://fashion-studio.dicoding.dev",
                total_pages=50,
                delay=0.5  # 0.5 detik delay antar halaman
            )
            
            logger.info(f"✓ Extract completed: {len(df_raw)} products scraped")
            
            # Save raw data untuk backup
            df_raw.to_csv("raw_products.csv", index=False)
            logger.info("✓ Raw data saved to: raw_products.csv")
            
        except Exception as e:
            logger.error(f"✗ Extract failed: {e}")
            raise
        
        # ============================================================
        # STEP 2: TRANSFORM
        # ============================================================
        logger.info("\n" + "="*70)
        logger.info("STEP 2: TRANSFORM - Cleaning and transforming data")
        logger.info("="*70)
        
        try:
            # Transform data
            df_clean = transform_data(df_raw)
            
            logger.info(f"✓ Transform completed: {len(df_clean)} valid products")
            logger.info(f"  - Removed: {len(df_raw) - len(df_clean)} invalid/duplicate/null rows")
            
            # Save clean data untuk backup
            df_clean.to_csv("clean_products.csv", index=False)
            logger.info("✓ Clean data saved to: clean_products.csv")
            
        except Exception as e:
            logger.error(f"✗ Transform failed: {e}")
            raise
        
        # ============================================================
        # STEP 3: LOAD
        # ============================================================
        logger.info("\n" + "="*70)
        logger.info("STEP 3: LOAD - Saving data to repositories")
        logger.info("="*70)
        
        try:
            # Configuration
            GOOGLE_SHEETS_ID = "19p-1wqJ1fkplCMBVyELMthm1Pvjc-hCAqlf0By9W5cY"
            
            # PostgreSQL config (set None jika tidak digunakan)
            POSTGRESQL_CONFIG = None
            
            # Uncomment dan isi jika mau pakai PostgreSQL:
            # POSTGRESQL_CONFIG = {
            #     'host': 'localhost',
            #     'port': 5432,
            #     'database': 'fashion_products',
            #     'user': 'your_username',
            #     'password': 'your_password'
            # }
            
            # Load to all repositories
            results = load_data_to_all_repositories(
                df=df_clean,
                csv_filename="products.csv",
                google_sheets_id=GOOGLE_SHEETS_ID,
                google_credentials_file="google-sheets-api.json",
                postgresql_config=POSTGRESQL_CONFIG
            )
            
            logger.info("✓ Load completed")
            
        except Exception as e:
            logger.error(f"✗ Load failed: {e}")
            raise
        
        # ============================================================
        # SUMMARY
        # ============================================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*70)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*70)
        logger.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
        logger.info(f"\nData Summary:")
        logger.info(f"  - Raw data scraped: {len(df_raw)} rows")
        logger.info(f"  - Clean data output: {len(df_clean)} rows")
        logger.info(f"  - Data removed: {len(df_raw) - len(df_clean)} rows")
        logger.info(f"\nRepositories:")
        for repo, result in results.items():
            status = "✓" if result['status'] == 'success' else "○" if result['status'] == 'skipped' else "✗"
            logger.info(f"  {status} {repo.upper()}: {result['status']}")
            if result['info']:
                logger.info(f"     → {result['info']}")
        
        logger.info("\n" + "="*70)
        logger.info("Pipeline logs saved to: etl_pipeline_*.log")
        logger.info("="*70)
        
        return {
            'status': 'success',
            'raw_count': len(df_raw),
            'clean_count': len(df_clean),
            'duration': duration,
            'repositories': results
        }
        
    except Exception as e:
        logger.error("\n" + "="*70)
        logger.error("ETL PIPELINE FAILED")
        logger.error("="*70)
        logger.error(f"Error: {e}")
        logger.error("="*70)
        
        return {
            'status': 'failed',
            'error': str(e)
        }


def main():
    """
    Entry point untuk menjalankan ETL pipeline
    """
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "FASHION STUDIO ETL PIPELINE" + " "*26 + "║")
    print("║" + " "*20 + "Data Engineering Project" + " "*24 + "║")
    print("╚" + "="*68 + "╝")
    print("\n")
    
    try:
        # Run ETL pipeline
        result = run_etl_pipeline()
        
        # Exit with appropriate code
        if result['status'] == 'success':
            logger.info("\n✓ ETL Pipeline executed successfully!")
            sys.exit(0)
        else:
            logger.error("\n✗ ETL Pipeline execution failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\n\n⚠ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n✗ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()