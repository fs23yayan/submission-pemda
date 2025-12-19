"""
Module untuk transformasi dan pembersihan data produk
Mengubah format data mentah menjadi data yang siap pakai
"""

import pandas as pd
import re
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_price_to_rupiah(df, exchange_rate=16000):
    """
    Konversi harga dari dollar ke rupiah
    
    Args:
        df (pd.DataFrame): DataFrame dengan kolom 'Price'
        exchange_rate (int): Nilai tukar dollar ke rupiah
        
    Returns:
        pd.DataFrame: DataFrame dengan Price dalam rupiah
        
    Raises:
        Exception: Jika terjadi error saat konversi
    """
    try:
        logger.info("Converting price from USD to IDR...")
        
        def convert_price(price_str):
            """Helper function untuk konversi satu nilai price"""
            try:
                # Remove $ dan konversi ke float
                if isinstance(price_str, str) and '$' in price_str:
                    # Extract angka dari string seperti "$100.00"
                    price_usd = float(price_str.replace('$', '').replace(',', '').strip())
                    price_idr = price_usd * exchange_rate
                    return price_idr
                else:
                    # Jika tidak ada $, return None (akan di-handle sebagai invalid)
                    return None
            except Exception as e:
                logger.warning(f"Error converting price '{price_str}': {e}")
                return None
        
        # Apply conversion
        df['Price'] = df['Price'].apply(convert_price)
        
        # Count successful conversions
        valid_prices = df['Price'].notna().sum()
        logger.info(f"Successfully converted {valid_prices} prices to IDR")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in convert_price_to_rupiah: {e}")
        raise


def clean_rating(df):
    """
    Bersihkan kolom Rating dari format "Rating: ⭐ 3.9 / 5" menjadi 3.9
    
    Args:
        df (pd.DataFrame): DataFrame dengan kolom 'Rating'
        
    Returns:
        pd.DataFrame: DataFrame dengan Rating sebagai float
        
    Raises:
        Exception: Jika terjadi error saat cleaning
    """
    try:
        logger.info("Cleaning Rating column...")
        
        def extract_rating(rating_str):
            """Helper function untuk extract rating number"""
            try:
                if pd.isna(rating_str):
                    return None
                
                # Extract angka dari string seperti "Rating: ⭐ 3.9 / 5"
                # Pattern: cari angka desimal antara 0-5
                match = re.search(r'(\d+\.?\d*)\s*/', str(rating_str))
                if match:
                    rating_value = float(match.group(1))
                    # Validasi range 0-5
                    if 0 <= rating_value <= 5:
                        return rating_value
                
                # Jika tidak ada pattern yang cocok atau invalid
                return None
                
            except Exception as e:
                logger.warning(f"Error extracting rating from '{rating_str}': {e}")
                return None
        
        # Apply cleaning
        df['Rating'] = df['Rating'].apply(extract_rating)
        
        # Count valid ratings
        valid_ratings = df['Rating'].notna().sum()
        logger.info(f"Successfully cleaned {valid_ratings} ratings")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in clean_rating: {e}")
        raise


def clean_colors(df):
    """
    Bersihkan kolom Colors dari format "3 Colors" menjadi 3
    
    Args:
        df (pd.DataFrame): DataFrame dengan kolom 'Colors'
        
    Returns:
        pd.DataFrame: DataFrame dengan Colors sebagai integer
        
    Raises:
        Exception: Jika terjadi error saat cleaning
    """
    try:
        logger.info("Cleaning Colors column...")
        
        def extract_color_count(color_str):
            """Helper function untuk extract jumlah warna"""
            try:
                if pd.isna(color_str):
                    return None
                
                # Extract angka dari string seperti "3 Colors"
                match = re.search(r'(\d+)', str(color_str))
                if match:
                    color_count = int(match.group(1))
                    return color_count
                
                return None
                
            except Exception as e:
                logger.warning(f"Error extracting colors from '{color_str}': {e}")
                return None
        
        # Apply cleaning
        df['Colors'] = df['Colors'].apply(extract_color_count)
        
        # Count valid colors
        valid_colors = df['Colors'].notna().sum()
        logger.info(f"Successfully cleaned {valid_colors} color values")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in clean_colors: {e}")
        raise


def clean_size(df):
    """
    Bersihkan kolom Size dari format "Size: M" menjadi "M"
    
    Args:
        df (pd.DataFrame): DataFrame dengan kolom 'Size'
        
    Returns:
        pd.DataFrame: DataFrame dengan Size yang sudah dibersihkan
        
    Raises:
        Exception: Jika terjadi error saat cleaning
    """
    try:
        logger.info("Cleaning Size column...")
        
        def extract_size(size_str):
            """Helper function untuk extract ukuran"""
            try:
                if pd.isna(size_str):
                    return None
                
                # Remove "Size: " prefix
                size_str = str(size_str).strip()
                size_clean = size_str.replace('Size:', '').strip()
                
                # Validasi ukuran yang valid (S, M, L, XL, XXL, dll)
                valid_sizes = ['S', 'M', 'L', 'XL', 'XXL', 'XXXL', 'XS']
                if size_clean.upper() in valid_sizes:
                    return size_clean
                
                # Jika tidak valid atau "Unknown"
                return None
                
            except Exception as e:
                logger.warning(f"Error extracting size from '{size_str}': {e}")
                return None
        
        # Apply cleaning
        df['Size'] = df['Size'].apply(extract_size)
        
        # Count valid sizes
        valid_sizes = df['Size'].notna().sum()
        logger.info(f"Successfully cleaned {valid_sizes} size values")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in clean_size: {e}")
        raise


def clean_gender(df):
    """
    Bersihkan kolom Gender dari format "Gender: Men" menjadi "Men"
    
    Args:
        df (pd.DataFrame): DataFrame dengan kolom 'Gender'
        
    Returns:
        pd.DataFrame: DataFrame dengan Gender yang sudah dibersihkan
        
    Raises:
        Exception: Jika terjadi error saat cleaning
    """
    try:
        logger.info("Cleaning Gender column...")
        
        def extract_gender(gender_str):
            """Helper function untuk extract gender"""
            try:
                if pd.isna(gender_str):
                    return None
                
                # Remove "Gender: " prefix
                gender_str = str(gender_str).strip()
                gender_clean = gender_str.replace('Gender:', '').strip()
                
                # Validasi gender yang valid
                valid_genders = ['Men', 'Women', 'Unisex']
                if gender_clean in valid_genders:
                    return gender_clean
                
                # Jika tidak valid atau "Unknown"
                return None
                
            except Exception as e:
                logger.warning(f"Error extracting gender from '{gender_str}': {e}")
                return None
        
        # Apply cleaning
        df['Gender'] = df['Gender'].apply(extract_gender)
        
        # Count valid genders
        valid_genders = df['Gender'].notna().sum()
        logger.info(f"Successfully cleaned {valid_genders} gender values")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in clean_gender: {e}")
        raise


def remove_invalid_data(df):
    """
    Hapus data dengan nilai invalid seperti "Unknown Product"
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame tanpa data invalid
        
    Raises:
        Exception: Jika terjadi error saat menghapus data
    """
    try:
        logger.info("Removing invalid data...")
        initial_count = len(df)
        
        # Remove rows dengan Title "Unknown Product"
        df = df[df['Title'] != 'Unknown Product']
        
        logger.info(f"Removed {initial_count - len(df)} rows with invalid data")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in remove_invalid_data: {e}")
        raise


def remove_duplicates(df):
    """
    Hapus data duplikat
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame tanpa duplikat
        
    Raises:
        Exception: Jika terjadi error saat menghapus duplikat
    """
    try:
        logger.info("Removing duplicates...")
        initial_count = len(df)
        
        # Drop duplicates berdasarkan semua kolom kecuali Timestamp
        columns_to_check = [col for col in df.columns if col != 'Timestamp']
        df = df.drop_duplicates(subset=columns_to_check, keep='first')
        
        logger.info(f"Removed {initial_count - len(df)} duplicate rows")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in remove_duplicates: {e}")
        raise


def remove_null_values(df):
    """
    Hapus data dengan nilai null
    
    Args:
        df (pd.DataFrame): DataFrame yang akan dibersihkan
        
    Returns:
        pd.DataFrame: DataFrame tanpa nilai null
        
    Raises:
        Exception: Jika terjadi error saat menghapus null
    """
    try:
        logger.info("Removing null values...")
        initial_count = len(df)
        
        # Drop rows dengan nilai null
        df = df.dropna()
        
        logger.info(f"Removed {initial_count - len(df)} rows with null values")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in remove_null_values: {e}")
        raise


def transform_data(df):
    """
    Fungsi utama untuk transformasi data
    Menjalankan semua proses cleaning dan transformasi
    
    Args:
        df (pd.DataFrame): DataFrame mentah dari extract
        
    Returns:
        pd.DataFrame: DataFrame yang sudah ditransformasi
        
    Raises:
        Exception: Jika terjadi error kritis saat transformasi
    """
    try:
        logger.info("="*60)
        logger.info("STARTING DATA TRANSFORMATION")
        logger.info("="*60)
        logger.info(f"Initial data shape: {df.shape}")
        
        # 1. Remove invalid data first
        df = remove_invalid_data(df)
        logger.info(f"After removing invalid: {df.shape}")
        
        # 2. Convert price to rupiah
        df = convert_price_to_rupiah(df, exchange_rate=16000)
        logger.info(f"After price conversion: {df.shape}")
        
        # 3. Clean all columns
        df = clean_rating(df)
        df = clean_colors(df)
        df = clean_size(df)
        df = clean_gender(df)
        logger.info(f"After cleaning all columns: {df.shape}")
        
        # 4. Remove null values (dari cleaning)
        df = remove_null_values(df)
        logger.info(f"After removing nulls: {df.shape}")
        
        # 5. Remove duplicates
        df = remove_duplicates(df)
        logger.info(f"After removing duplicates: {df.shape}")
        
        # 6. Convert data types
        df = df.astype({
            'Title': 'string',
            'Price': 'float64',
            'Rating': 'float64',
            'Colors': 'int64',
            'Size': 'string',
            'Gender': 'string',
            'Timestamp': 'string'
        })
        
        logger.info("="*60)
        logger.info("TRANSFORMATION COMPLETED")
        logger.info("="*60)
        logger.info(f"Final data shape: {df.shape}")
        logger.info(f"Data types:\n{df.dtypes}")
        
        return df
        
    except Exception as e:
        logger.error(f"Critical error in transform_data: {e}")
        raise


def main():
    """
    Fungsi main untuk testing module transform
    """
    try:
        # Load data dari extract
        # input_file = "raw_products_test.csv"
        input_file = "raw_products.csv"
        logger.info(f"Loading data from {input_file}...")
        df = pd.read_csv(input_file)
        
        print("\n" + "="*60)
        print("BEFORE TRANSFORMATION")
        print("="*60)
        print(f"Shape: {df.shape}")
        print(f"\nFirst 5 rows:")
        print(df.head())
        print(f"\nData types:")
        print(df.dtypes)
        
        # Transform data
        df_clean = transform_data(df)
        
        print("\n" + "="*60)
        print("AFTER TRANSFORMATION")
        print("="*60)
        print(f"Shape: {df_clean.shape}")
        print(f"\nFirst 5 rows:")
        print(df_clean.head())
        print(f"\nData types:")
        print(df_clean.dtypes)
        print(f"\nSummary statistics:")
        print(df_clean.describe())
        
        # Save cleaned data
        # output_file = "clean_products_test.csv"
        output_file = "clean_products.csv"
        df_clean.to_csv(output_file, index=False)
        logger.info(f"\nCleaned data saved to {output_file}")
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()