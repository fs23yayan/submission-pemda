"""
Unit tests untuk module extract
Testing fungsi-fungsi scraping dan extraction
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from bs4 import BeautifulSoup
from utils.extract import (
    scrape_product_data,
    extract_product_info,
    scrape_all_pages
)


class TestExtractProductInfo:
    """Test cases untuk fungsi extract_product_info"""
    
    def test_extract_valid_product(self):
        """Test extract produk dengan data lengkap dan valid"""
        html = """
        <div>
            <h3 class="product-title">Test Product</h3>
            <div class="price-container">
                <span class="price">$100.00</span>
            </div>
            <p>Rating: ⭐ 4.5 / 5</p>
            <p>3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h3', class_='product-title')
        
        result = extract_product_info(title_elem)
        
        assert result is not None
        assert result['Title'] == 'Test Product'
        assert result['Price'] == '$100.00'
        assert 'Rating:' in result['Rating']
        assert 'Colors' in result['Colors']
        assert 'Size:' in result['Size']
        assert 'Gender:' in result['Gender']
        assert 'Timestamp' in result
    
    def test_extract_product_with_price_unavailable(self):
        """Test extract produk dengan harga tidak tersedia"""
        html = """
        <div>
            <h3 class="product-title">Test Product</h3>
            <div class="price-container">
                <span class="price">Price Unavailable</span>
            </div>
            <p>Rating: ⭐ 4.5 / 5</p>
            <p>3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h3', class_='product-title')
        
        result = extract_product_info(title_elem)
        
        assert result is not None
        assert 'Price Unavailable' in result['Price']
    
    def test_extract_product_with_missing_price_container(self):
        """Test extract produk tanpa price container"""
        html = """
        <div>
            <h3 class="product-title">Test Product</h3>
            <p>Rating: ⭐ 4.5 / 5</p>
            <p>3 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h3', class_='product-title')
        
        result = extract_product_info(title_elem)
        
        assert result is not None
        assert result['Price'] == 'Price Unavailable'
    
    def test_extract_product_with_invalid_rating(self):
        """Test extract produk dengan rating invalid"""
        html = """
        <div>
            <h3 class="product-title">Unknown Product</h3>
            <div class="price-container">
                <span class="price">$100.00</span>
            </div>
            <p>Rating: Invalid Rating</p>
            <p>5 Colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
        </div>
        """
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h3', class_='product-title')
        
        result = extract_product_info(title_elem)
        
        assert result is not None
        assert 'Invalid Rating' in result['Rating'] or result['Rating'] == 'Invalid Rating'
    
    def test_extract_product_with_error_handling(self):
        """Test error handling saat extract gagal"""
        # Create invalid element that will cause error
        invalid_elem = None
        
        result = extract_product_info(invalid_elem)
        
        # Should return None when error occurs
        assert result is None


class TestScrapeProductData:
    """Test cases untuk fungsi scrape_product_data"""
    
    @patch('utils.extract.requests.Session')
    def test_scrape_product_data_success(self, mock_session):
        """Test scraping berhasil dengan response valid"""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = """
        <html>
            <body>
                <h3 class="product-title">Product 1</h3>
                <div class="price-container">
                    <span class="price">$100.00</span>
                </div>
                <p>Rating: ⭐ 4.5 / 5</p>
                <p>3 Colors</p>
                <p>Size: M</p>
                <p>Gender: Men</p>
                
                <h3 class="product-title">Product 2</h3>
                <div class="price-container">
                    <span class="price">$200.00</span>
                </div>
                <p>Rating: ⭐ 4.0 / 5</p>
                <p>2 Colors</p>
                <p>Size: L</p>
                <p>Gender: Women</p>
            </body>
        </html>
        """.encode('utf-8')
        
        mock_session_instance = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        result = scrape_product_data("https://test.com")
        
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]['Title'] == 'Product 1'
        assert result[1]['Title'] == 'Product 2'
    
    @patch('utils.extract.requests.Session')
    def test_scrape_product_data_no_products(self, mock_session):
        """Test scraping halaman tanpa produk"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"<html><body><p>No products</p></body></html>"
        
        mock_session_instance = Mock()
        mock_session_instance.get.return_value = mock_response
        mock_session.return_value = mock_session_instance
        
        result = scrape_product_data("https://test.com")
        
        assert isinstance(result, list)
        assert len(result) == 0
    
    @patch('utils.extract.requests.Session')
    @patch('utils.extract.time.sleep')
    def test_scrape_product_data_with_retry(self, mock_sleep, mock_session):
        """Test retry mechanism saat request gagal"""
        mock_session_instance = Mock()
        
        # Create proper mock response for 3rd attempt
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"<html><body><h3 class='product-title'>Test</h3></body></html>"
        
        # Use RequestException instead of generic Exception
        from requests.exceptions import RequestException
        
        # First 2 attempts fail, 3rd succeeds
        mock_session_instance.get.side_effect = [
            RequestException("Connection error"),
            RequestException("Timeout"),
            mock_response
        ]
        
        mock_session.return_value = mock_session_instance
        
        result = scrape_product_data("https://test.com", max_retries=3)
        
        assert isinstance(result, list)
        assert mock_session_instance.get.call_count == 3
    
    @patch('utils.extract.requests.Session')
    def test_scrape_product_data_failure_after_retries(self, mock_session):
        """Test kegagalan setelah semua retry"""
        from requests.exceptions import RequestException
        
        mock_session_instance = Mock()
        mock_session_instance.get.side_effect = RequestException("Connection error")
        mock_session.return_value = mock_session_instance
        
        with pytest.raises(Exception):
            scrape_product_data("https://test.com", max_retries=3)


class TestScrapeAllPages:
    """Test cases untuk fungsi scrape_all_pages"""
    
    @patch('utils.extract.scrape_product_data')
    @patch('utils.extract.time.sleep')
    def test_scrape_all_pages_success(self, mock_sleep, mock_scrape):
        """Test scraping multiple pages berhasil"""
        # Mock scrape_product_data untuk return data
        mock_scrape.return_value = [
            {
                'Title': 'Product 1',
                'Price': '$100.00',
                'Rating': 'Rating: ⭐ 4.5 / 5',
                'Colors': '3 Colors',
                'Size': 'Size: M',
                'Gender': 'Gender: Men',
                'Timestamp': '2024-01-01 00:00:00'
            }
        ]
        
        result = scrape_all_pages(
            base_url="https://test.com",
            total_pages=3,
            delay=0.1
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3  # 3 pages × 1 product per page
        assert mock_scrape.call_count == 3
        assert 'Title' in result.columns
        assert 'Price' in result.columns
    
    @patch('utils.extract.scrape_product_data')
    @patch('utils.extract.time.sleep')
    def test_scrape_all_pages_with_failed_pages(self, mock_sleep, mock_scrape):
        """Test scraping dengan beberapa halaman gagal"""
        # Page 1 success, page 2 fails, page 3 success
        mock_scrape.side_effect = [
            [{'Title': 'Product 1', 'Price': '$100', 'Rating': '4.5', 
              'Colors': '3', 'Size': 'M', 'Gender': 'Men', 'Timestamp': '2024-01-01'}],
            Exception("Error on page 2"),
            [{'Title': 'Product 3', 'Price': '$300', 'Rating': '4.0',
              'Colors': '2', 'Size': 'L', 'Gender': 'Women', 'Timestamp': '2024-01-01'}]
        ]
        
        result = scrape_all_pages(
            base_url="https://test.com",
            total_pages=3,
            delay=0.1
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Only 2 successful pages
    
    @patch('utils.extract.scrape_product_data')
    def test_scrape_all_pages_all_failed(self, mock_scrape):
        """Test semua halaman gagal"""
        mock_scrape.return_value = []
        
        with pytest.raises(Exception, match="No products scraped"):
            scrape_all_pages(
                base_url="https://test.com",
                total_pages=2,
                delay=0.1
            )
    
    @patch('utils.extract.scrape_product_data')
    @patch('utils.extract.time.sleep')
    def test_scrape_all_pages_url_construction(self, mock_sleep, mock_scrape):
        """Test konstruksi URL untuk berbagai halaman"""
        mock_scrape.return_value = [
            {'Title': 'P', 'Price': '$1', 'Rating': '4', 'Colors': '1',
             'Size': 'M', 'Gender': 'Men', 'Timestamp': '2024-01-01'}
        ]
        
        scrape_all_pages(
            base_url="https://test.com",
            total_pages=3,
            delay=0.1
        )
        
        # Check URL calls
        calls = mock_scrape.call_args_list
        assert calls[0][0][0] == "https://test.com"  # Page 1
        assert calls[1][0][0] == "https://test.com/page2"  # Page 2
        assert calls[2][0][0] == "https://test.com/page3"  # Page 3


class TestErrorHandling:
    """Test cases untuk error handling"""
    
    def test_extract_with_none_element(self):
        """Test extract dengan element None"""
        result = extract_product_info(None)
        assert result is None
    
    @patch('utils.extract.requests.Session')
    def test_network_error_handling(self, mock_session):
        """Test handling network errors"""
        mock_session_instance = Mock()
        mock_session_instance.get.side_effect = ConnectionError("Network error")
        mock_session.return_value = mock_session_instance
        
        with pytest.raises(Exception):
            scrape_product_data("https://test.com", max_retries=1)


# Pytest fixtures untuk reusable test data
@pytest.fixture
def sample_html():
    """Sample HTML untuk testing"""
    return """
    <div>
        <h3 class="product-title">Test Product</h3>
        <div class="price-container">
            <span class="price">$100.00</span>
        </div>
        <p>Rating: ⭐ 4.5 / 5</p>
        <p>3 Colors</p>
        <p>Size: M</p>
        <p>Gender: Men</p>
    </div>
    """


@pytest.fixture
def sample_product_data():
    """Sample product data untuk testing"""
    return {
        'Title': 'Test Product',
        'Price': '$100.00',
        'Rating': 'Rating: ⭐ 4.5 / 5',
        'Colors': '3 Colors',
        'Size': 'Size: M',
        'Gender': 'Gender: Men',
        'Timestamp': '2024-01-01 00:00:00'
    }