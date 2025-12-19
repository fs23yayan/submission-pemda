"""
Module untuk ekstraksi data dari website Fashion Studio
Mengambil data produk fashion dari https://fashion-studio.dicoding.dev/
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import logging
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def scrape_product_data(url, max_retries=3):
    """
    Scrape data dari satu halaman produk
    
    Args:
        url (str): URL halaman yang akan di-scrape
        max_retries (int): Maksimal percobaan request jika gagal
        
    Returns:
        list: List berisi dictionary data produk
        
    Raises:
        Exception: Jika request gagal setelah max_retries
    """
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, timeout=10)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
                    raise Exception(f"Request failed: {e}")
                logger.warning(f"Attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        products = []
        
        # Cari semua h3 yang merupakan product title
        product_titles = soup.find_all('h3', class_='product-title')
        
        if not product_titles:
            logger.warning(f"No products found on {url}")
            return products
        
        # Extract data dari setiap produk
        for title_elem in product_titles:
            try:
                product = extract_product_info(title_elem)
                if product:
                    products.append(product)
            except Exception as e:
                logger.error(f"Error extracting product: {e}")
                continue
        
        logger.info(f"Successfully scraped {len(products)} products from {url}")
        return products
        
    except Exception as e:
        logger.error(f"Error in scrape_product_data: {e}")
        raise


def extract_product_info(title_elem):
    """
    Extract informasi produk berdasarkan struktur HTML actual
    Struktur: 
    - h3 class="product-title" (title)
    - div class="price-container" > span class="price" (price)
    - p tags (rating, colors, size, gender)
    
    Args:
        title_elem: BeautifulSoup element dari h3 title
        
    Returns:
        dict: Dictionary berisi informasi produk atau None jika gagal
    """
    try:
        product = {}
        
        # Extract Title
        product['Title'] = title_elem.text.strip()
        
        # Cari sibling elements
        current = title_elem.find_next_sibling()
        
        price_found = False
        rating_found = False
        colors_found = False
        size_found = False
        gender_found = False
        
        # Iterasi melalui siblings untuk menemukan data
        while current and not all([price_found, rating_found, colors_found, size_found, gender_found]):
            # Check if current is div with class price-container
            if current.name == 'div' and current.get('class') and 'price-container' in current.get('class', []):
                # Extract price dari span dengan class 'price'
                price_span = current.find('span', class_='price')
                if price_span:
                    product['Price'] = price_span.text.strip()
                    price_found = True
                else:
                    # Jika tidak ada span, ambil text langsung
                    text = current.text.strip()
                    if '$' in text or 'Price Unavailable' in text:
                        product['Price'] = text
                        price_found = True
            
            elif current.name == 'p':
                text = current.text.strip()
                
                # Detect Rating (contains "Rating:")
                if not rating_found and ('Rating:' in text or 'Not Rated' in text):
                    product['Rating'] = text
                    rating_found = True
                
                # Detect Colors (contains "Colors")
                elif not colors_found and 'Colors' in text:
                    product['Colors'] = text
                    colors_found = True
                
                # Detect Size (contains "Size:")
                elif not size_found and 'Size:' in text:
                    product['Size'] = text
                    size_found = True
                
                # Detect Gender (contains "Gender:")
                elif not gender_found and 'Gender:' in text:
                    product['Gender'] = text
                    gender_found = True
            
            # Hentikan jika sudah menemukan h3 berikutnya (produk baru)
            elif current.name == 'h3':
                break
            
            current = current.find_next_sibling()
        
        # Set default values untuk data yang tidak ditemukan
        if not price_found:
            product['Price'] = 'Price Unavailable'
        if not rating_found:
            product['Rating'] = 'Invalid Rating'
        if not colors_found:
            product['Colors'] = '0 Colors'
        if not size_found:
            product['Size'] = 'Size: Unknown'
        if not gender_found:
            product['Gender'] = 'Gender: Unknown'
        
        # Add timestamp
        product['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return product
        
    except Exception as e:
        logger.error(f"Error extracting product info: {e}")
        return None


def scrape_all_pages(base_url="https://fashion-studio.dicoding.dev", 
                     total_pages=50, 
                     delay=0.5):
    """
    Scrape semua halaman website
    
    Args:
        base_url (str): Base URL website
        total_pages (int): Jumlah total halaman yang akan di-scrape
        delay (float): Delay antar request dalam detik
        
    Returns:
        pd.DataFrame: DataFrame berisi semua data produk
        
    Raises:
        Exception: Jika terjadi error kritis saat scraping
    """
    try:
        all_products = []
        failed_pages = []
        
        logger.info(f"Starting to scrape {total_pages} pages from {base_url}")
        
        for page in range(1, total_pages + 1):
            try:
                # Construct URL untuk setiap halaman
                if page == 1:
                    url = base_url
                else:
                    # Berdasarkan struktur pagination: /page2, /page3, dst
                    url = f"{base_url}/page{page}"
                
                logger.info(f"Scraping page {page}/{total_pages}: {url}")
                
                # Scrape halaman
                products = scrape_product_data(url)
                
                if products:
                    all_products.extend(products)
                    logger.info(f"Page {page}: Found {len(products)} products")
                else:
                    logger.warning(f"Page {page}: No products found")
                    failed_pages.append(page)
                
                # Delay untuk menghindari overload server
                if page < total_pages:
                    time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Error scraping page {page}: {e}")
                failed_pages.append(page)
                # Lanjutkan ke halaman berikutnya
                continue
        
        if not all_products:
            raise Exception("No products scraped from any page")
        
        # Convert ke DataFrame
        df = pd.DataFrame(all_products)
        
        logger.info("="*60)
        logger.info(f"SCRAPING COMPLETED")
        logger.info("="*60)
        logger.info(f"Total products scraped: {len(df)}")
        logger.info(f"Successful pages: {total_pages - len(failed_pages)}/{total_pages}")
        if failed_pages:
            logger.warning(f"Failed pages: {failed_pages}")
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Critical error in scrape_all_pages: {e}")
        raise


def main():
    """
    Fungsi main untuk testing module extract
    """
    try:
        logger.info("="*60)
        logger.info("STARTING EXTRACTION PROCESS")
        logger.info("="*60)
        
        # Test dengan 2 halaman dulu
        # logger.info("Testing with 2 pages first...")
        # df_test = scrape_all_pages(
        #     base_url="https://fashion-studio.dicoding.dev",
        #     total_pages=2,
        #     delay=0.5
        # )
        
        # print("\n" + "="*60)
        # print("TEST EXTRACTION SUMMARY")
        # print("="*60)
        # print(f"Total products scraped: {len(df_test)}")
        # print(f"\nFirst 5 rows:")
        # print(df_test.head())
        # print(f"\nData types:")
        # print(df_test.dtypes)
        # print(f"\nNull values:")
        # print(df_test.isnull().sum())
        # print(f"\nValue counts for key columns:")
        # print(f"\nPrice samples:")
        # print(df_test['Price'].value_counts().head())
        # print(f"\nRating samples:")
        # print(df_test['Rating'].value_counts().head())
        
        # Jika test berhasil, uncomment ini untuk full scraping
        logger.info("\nStarting FULL extraction (50 pages)...")
        df = scrape_all_pages(
            base_url="https://fashion-studio.dicoding.dev",
            total_pages=50,
            delay=0.5
        )
        
        # Save test data
        # output_file = "raw_products_test.csv"
        # df_test.to_csv(output_file, index=False)
        # logger.info(f"\nTest data saved to {output_file}")
        
        # return df_test

        output_file = "raw_products.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"\nTest data saved to {output_file}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()