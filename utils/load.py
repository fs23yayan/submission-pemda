"""
Module untuk loading data ke berbagai repositori
Menyimpan data ke CSV, Google Sheets, dan PostgreSQL
"""

import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def save_to_csv(df, filename="products.csv"):
    """
    Simpan DataFrame ke file CSV
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        filename (str): Nama file output
        
    Returns:
        str: Path file yang disimpan
        
    Raises:
        Exception: Jika terjadi error saat menyimpan
    """
    try:
        logger.info(f"Saving data to CSV: {filename}")
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        # Verify file exists
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            logger.info(f"Successfully saved {len(df)} rows to {filename}")
            logger.info(f"File size: {file_size} bytes")
            return filename
        else:
            raise Exception(f"Failed to create file: {filename}")
        
    except Exception as e:
        logger.error(f"Error in save_to_csv: {e}")
        raise


def save_to_google_sheets(df, spreadsheet_id, credentials_file="google-sheets-api.json", 
                          sheet_name="Products"):
    """
    Simpan DataFrame ke Google Sheets
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        spreadsheet_id (str): ID Google Spreadsheet
        credentials_file (str): Path ke file credentials JSON
        sheet_name (str): Nama sheet/tab
        
    Returns:
        str: URL Google Sheets
        
    Raises:
        Exception: Jika terjadi error saat menyimpan
    """
    try:
        logger.info("Saving data to Google Sheets...")
        
        # Load credentials
        if not os.path.exists(credentials_file):
            raise Exception(f"Credentials file not found: {credentials_file}")
        
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES
        )
        
        # Build service
        service = build('sheets', 'v4', credentials=credentials)
        
        # Get spreadsheet info to check existing sheets
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
        
        logger.info(f"Existing sheets: {existing_sheets}")
        
        # If sheet doesn't exist, create it or use first available sheet
        if sheet_name not in existing_sheets:
            logger.warning(f"Sheet '{sheet_name}' not found")
            
            # Try to create new sheet
            try:
                request_body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=request_body
                ).execute()
                logger.info(f"Created new sheet: {sheet_name}")
            except Exception as e:
                # If can't create, use first existing sheet
                sheet_name = existing_sheets[0]
                logger.warning(f"Using existing sheet instead: {sheet_name}")
        
        # Prepare data: convert DataFrame to list of lists
        # Include header
        values = [df.columns.tolist()] + df.values.tolist()
        
        # Clear existing data first
        try:
            clear_request = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A:Z"
            )
            clear_request.execute()
            logger.info(f"Cleared existing data in sheet: {sheet_name}")
        except Exception as e:
            logger.warning(f"Could not clear sheet (might be empty): {e}")
        
        # Write new data
        body = {
            'values': values
        }
        
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption='RAW',
            body=body
        ).execute()
        
        updated_cells = result.get('updatedCells', 0)
        logger.info(f"Successfully updated {updated_cells} cells in Google Sheets")
        
        # Generate URL
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
        logger.info(f"Google Sheets URL: {sheet_url}")
        
        return sheet_url
        
    except Exception as e:
        logger.error(f"Error in save_to_google_sheets: {e}")
        raise


def save_to_postgresql(df, db_config, table_name="products"):
    """
    Simpan DataFrame ke PostgreSQL database
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        db_config (dict): Dictionary berisi database configuration
            - host: Database host
            - port: Database port
            - database: Database name
            - user: Database user
            - password: Database password
        table_name (str): Nama tabel
        
    Returns:
        str: Informasi koneksi database
        
    Raises:
        Exception: Jika terjadi error saat menyimpan
    """
    try:
        logger.info("Saving data to PostgreSQL...")
        
        # Validate config
        required_keys = ['host', 'port', 'database', 'user', 'password']
        for key in required_keys:
            if key not in db_config:
                raise Exception(f"Missing required config: {key}")
        
        # Create connection string
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        # Create engine
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to PostgreSQL")
        
        # Save DataFrame to PostgreSQL
        # if_exists='replace' akan drop table jika ada dan create baru
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Verify data
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            logger.info(f"Successfully saved {count} rows to PostgreSQL table: {table_name}")
        
        # Close engine
        engine.dispose()
        
        db_info = f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        logger.info(f"Database: {db_info}")
        
        return db_info
        
    except Exception as e:
        logger.error(f"Error in save_to_postgresql: {e}")
        raise


def load_data_to_all_repositories(df, 
                                   csv_filename="products.csv",
                                   google_sheets_id=None,
                                   google_credentials_file="google-sheets-api.json",
                                   postgresql_config=None):
    """
    Fungsi utama untuk menyimpan data ke semua repositori
    
    Args:
        df (pd.DataFrame): DataFrame yang akan disimpan
        csv_filename (str): Nama file CSV output
        google_sheets_id (str): Google Spreadsheet ID (optional)
        google_credentials_file (str): Path ke credentials file
        postgresql_config (dict): PostgreSQL configuration (optional)
        
    Returns:
        dict: Dictionary berisi status untuk setiap repositori
    """
    try:
        logger.info("="*60)
        logger.info("STARTING DATA LOADING TO ALL REPOSITORIES")
        logger.info("="*60)
        
        results = {
            'csv': {'status': 'not_attempted', 'info': None},
            'google_sheets': {'status': 'not_attempted', 'info': None},
            'postgresql': {'status': 'not_attempted', 'info': None}
        }
        
        # 1. Save to CSV (Always)
        try:
            csv_path = save_to_csv(df, csv_filename)
            results['csv'] = {'status': 'success', 'info': csv_path}
        except Exception as e:
            results['csv'] = {'status': 'failed', 'info': str(e)}
            logger.error(f"Failed to save to CSV: {e}")
        
        # 2. Save to Google Sheets (if configured)
        if google_sheets_id:
            try:
                sheet_url = save_to_google_sheets(
                    df, 
                    google_sheets_id, 
                    google_credentials_file
                )
                results['google_sheets'] = {'status': 'success', 'info': sheet_url}
            except Exception as e:
                results['google_sheets'] = {'status': 'failed', 'info': str(e)}
                logger.error(f"Failed to save to Google Sheets: {e}")
        else:
            results['google_sheets'] = {'status': 'skipped', 'info': 'No spreadsheet ID provided'}
            logger.warning("Skipping Google Sheets (no spreadsheet ID)")
        
        # 3. Save to PostgreSQL (if configured)
        if postgresql_config:
            try:
                db_info = save_to_postgresql(df, postgresql_config)
                results['postgresql'] = {'status': 'success', 'info': db_info}
            except Exception as e:
                results['postgresql'] = {'status': 'failed', 'info': str(e)}
                logger.error(f"Failed to save to PostgreSQL: {e}")
        else:
            results['postgresql'] = {'status': 'skipped', 'info': 'No database config provided'}
            logger.warning("Skipping PostgreSQL (no database config)")
        
        # Summary
        logger.info("="*60)
        logger.info("LOADING SUMMARY")
        logger.info("="*60)
        for repo, result in results.items():
            status_symbol = "✓" if result['status'] == 'success' else "✗" if result['status'] == 'failed' else "○"
            logger.info(f"{status_symbol} {repo.upper()}: {result['status']}")
            if result['info']:
                logger.info(f"  → {result['info']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Critical error in load_data_to_all_repositories: {e}")
        raise


def main():
    """
    Fungsi main untuk testing module load
    """
    try:
        # Load cleaned data
        input_file = "clean_products.csv"
        logger.info(f"Loading cleaned data from {input_file}...")
        df = pd.read_csv(input_file)
        
        print("\n" + "="*60)
        print("DATA TO BE LOADED")
        print("="*60)
        print(f"Shape: {df.shape}")
        print(f"\nFirst 5 rows:")
        print(df.head())
        
        # Configuration
        GOOGLE_SHEETS_ID = "19p-1wqJ1fkplCMBVyELMthm1Pvjc-hCAqlf0By9W5cY"
        
        # PostgreSQL config (CONFIGURE THIS!)
        # Option 1: Neon.tech (recommended)
        # Option 2: Local PostgreSQL
        # Option 3: ElephantSQL
        
        # POSTGRESQL_CONFIG = {
        #     'host': 'localhost',  # or your Neon.tech host
        #     'port': 5432,
        #     'database': 'fashion_products',
        #     'user': 'postgres',
        #     'password': 'your_password'  # CHANGE THIS!
        # }
        
        # Uncomment this if you don't have PostgreSQL yet
        POSTGRESQL_CONFIG = None
        
        # Load to all repositories
        results = load_data_to_all_repositories(
            df=df,
            csv_filename="products.csv",
            google_sheets_id=GOOGLE_SHEETS_ID,
            google_credentials_file="google-sheets-api.json",
            postgresql_config=POSTGRESQL_CONFIG  # Set to None to skip PostgreSQL
        )
        
        # Print results
        print("\n" + "="*60)
        print("FINAL RESULTS")
        print("="*60)
        for repo, result in results.items():
            print(f"\n{repo.upper()}:")
            print(f"  Status: {result['status']}")
            print(f"  Info: {result['info']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()