"""
Unit tests untuk module transform
Testing fungsi-fungsi transformasi dan cleaning data
"""

import pytest
import pandas as pd
import numpy as np
from utils.transform import (
    convert_price_to_rupiah,
    clean_rating,
    clean_colors,
    clean_size,
    clean_gender,
    remove_invalid_data,
    remove_duplicates,
    remove_null_values,
    transform_data
)


class TestConvertPriceToRupiah:
    """Test cases untuk fungsi convert_price_to_rupiah"""
    
    def test_convert_valid_price(self):
        """Test konversi harga valid"""
        df = pd.DataFrame({
            'Price': ['$100.00', '$50.50', '$200.00']
        })
        
        result = convert_price_to_rupiah(df, exchange_rate=16000)
        
        assert result['Price'][0] == 1600000.0
        assert result['Price'][1] == 808000.0
        assert result['Price'][2] == 3200000.0
    
    def test_convert_price_with_unavailable(self):
        """Test konversi dengan harga tidak tersedia"""
        df = pd.DataFrame({
            'Price': ['$100.00', 'Price Unavailable', '$200.00']
        })
        
        result = convert_price_to_rupiah(df)
        
        assert result['Price'][0] == 1600000.0
        assert pd.isna(result['Price'][1])
        assert result['Price'][2] == 3200000.0
    
    def test_convert_price_with_comma(self):
        """Test konversi harga dengan koma"""
        df = pd.DataFrame({
            'Price': ['$1,000.00', '$2,500.50']
        })
        
        result = convert_price_to_rupiah(df)
        
        assert result['Price'][0] == 16000000.0
        assert result['Price'][1] == 40008000.0
    
    def test_convert_price_custom_exchange_rate(self):
        """Test konversi dengan exchange rate custom"""
        df = pd.DataFrame({
            'Price': ['$100.00']
        })
        
        result = convert_price_to_rupiah(df, exchange_rate=15000)
        
        assert result['Price'][0] == 1500000.0
    
    def test_convert_price_error_handling(self):
        """Test error handling untuk data invalid"""
        df = pd.DataFrame({
            'Price': ['invalid', '$100.00', None]
        })
        
        result = convert_price_to_rupiah(df)
        
        assert pd.isna(result['Price'][0])
        assert result['Price'][1] == 1600000.0
        assert pd.isna(result['Price'][2])


class TestCleanRating:
    """Test cases untuk fungsi clean_rating"""
    
    def test_clean_valid_rating(self):
        """Test cleaning rating valid"""
        df = pd.DataFrame({
            'Rating': ['Rating: ⭐ 4.5 / 5', 'Rating: ⭐ 3.2 / 5', 'Rating: ⭐ 5.0 / 5']
        })
        
        result = clean_rating(df)
        
        assert result['Rating'][0] == 4.5
        assert result['Rating'][1] == 3.2
        assert result['Rating'][2] == 5.0
    
    def test_clean_invalid_rating(self):
        """Test cleaning rating invalid"""
        df = pd.DataFrame({
            'Rating': ['Invalid Rating', 'Rating: ⭐ 4.5 / 5']
        })
        
        result = clean_rating(df)
        
        assert pd.isna(result['Rating'][0])
        assert result['Rating'][1] == 4.5
    
    def test_clean_rating_not_rated(self):
        """Test cleaning rating 'Not Rated'"""
        df = pd.DataFrame({
            'Rating': ['Rating: Not Rated', 'Rating: ⭐ 3.5 / 5']
        })
        
        result = clean_rating(df)
        
        assert pd.isna(result['Rating'][0])
        assert result['Rating'][1] == 3.5
    
    def test_clean_rating_out_of_range(self):
        """Test rating di luar range 0-5"""
        df = pd.DataFrame({
            'Rating': ['Rating: ⭐ 6.0 / 5', 'Rating: Not Rated']
        })
        
        result = clean_rating(df)
        
        # Should return None for out of range and Not Rated
        assert pd.isna(result['Rating'][0])
        assert pd.isna(result['Rating'][1])
    
    def test_clean_rating_with_null(self):
        """Test cleaning dengan nilai null"""
        df = pd.DataFrame({
            'Rating': [None, 'Rating: ⭐ 4.0 / 5']
        })
        
        result = clean_rating(df)
        
        assert pd.isna(result['Rating'][0])
        assert result['Rating'][1] == 4.0


class TestCleanColors:
    """Test cases untuk fungsi clean_colors"""
    
    def test_clean_valid_colors(self):
        """Test cleaning colors valid"""
        df = pd.DataFrame({
            'Colors': ['3 Colors', '5 Colors', '2 Colors']
        })
        
        result = clean_colors(df)
        
        assert result['Colors'][0] == 3
        assert result['Colors'][1] == 5
        assert result['Colors'][2] == 2
    
    def test_clean_single_color(self):
        """Test cleaning single color"""
        df = pd.DataFrame({
            'Colors': ['1 Color', '10 Colors']
        })
        
        result = clean_colors(df)
        
        assert result['Colors'][0] == 1
        assert result['Colors'][1] == 10
    
    def test_clean_colors_invalid(self):
        """Test cleaning colors invalid"""
        df = pd.DataFrame({
            'Colors': ['No colors', '3 Colors', None]
        })
        
        result = clean_colors(df)
        
        assert pd.isna(result['Colors'][0])
        assert result['Colors'][1] == 3
        assert pd.isna(result['Colors'][2])


class TestCleanSize:
    """Test cases untuk fungsi clean_size"""
    
    def test_clean_valid_size(self):
        """Test cleaning size valid"""
        df = pd.DataFrame({
            'Size': ['Size: S', 'Size: M', 'Size: L', 'Size: XL', 'Size: XXL']
        })
        
        result = clean_size(df)
        
        assert result['Size'][0] == 'S'
        assert result['Size'][1] == 'M'
        assert result['Size'][2] == 'L'
        assert result['Size'][3] == 'XL'
        assert result['Size'][4] == 'XXL'
    
    def test_clean_size_unknown(self):
        """Test cleaning size unknown"""
        df = pd.DataFrame({
            'Size': ['Size: Unknown', 'Size: M']
        })
        
        result = clean_size(df)
        
        assert pd.isna(result['Size'][0])
        assert result['Size'][1] == 'M'
    
    def test_clean_size_case_insensitive(self):
        """Test cleaning size case insensitive"""
        df = pd.DataFrame({
            'Size': ['Size: m', 'Size: xl']
        })
        
        result = clean_size(df)
        
        assert result['Size'][0] == 'm'
        assert result['Size'][1] == 'xl'
    
    def test_clean_size_with_null(self):
        """Test cleaning size dengan null"""
        df = pd.DataFrame({
            'Size': [None, 'Size: L']
        })
        
        result = clean_size(df)
        
        assert pd.isna(result['Size'][0])
        assert result['Size'][1] == 'L'


class TestCleanGender:
    """Test cases untuk fungsi clean_gender"""
    
    def test_clean_valid_gender(self):
        """Test cleaning gender valid"""
        df = pd.DataFrame({
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex']
        })
        
        result = clean_gender(df)
        
        assert result['Gender'][0] == 'Men'
        assert result['Gender'][1] == 'Women'
        assert result['Gender'][2] == 'Unisex'
    
    def test_clean_gender_unknown(self):
        """Test cleaning gender unknown"""
        df = pd.DataFrame({
            'Gender': ['Gender: Unknown', 'Gender: Men']
        })
        
        result = clean_gender(df)
        
        assert pd.isna(result['Gender'][0])
        assert result['Gender'][1] == 'Men'
    
    def test_clean_gender_with_null(self):
        """Test cleaning gender dengan null"""
        df = pd.DataFrame({
            'Gender': [None, 'Gender: Women']
        })
        
        result = clean_gender(df)
        
        assert pd.isna(result['Gender'][0])
        assert result['Gender'][1] == 'Women'


class TestRemoveInvalidData:
    """Test cases untuk fungsi remove_invalid_data"""
    
    def test_remove_unknown_product(self):
        """Test remove produk dengan title Unknown Product"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Unknown Product', 'Product 3']
        })
        
        result = remove_invalid_data(df)
        
        assert len(result) == 2
        assert 'Unknown Product' not in result['Title'].values
    
    def test_remove_multiple_invalid(self):
        """Test remove multiple invalid products"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Unknown Product', 'Product 2', 'Unknown Product']
        })
        
        result = remove_invalid_data(df)
        
        assert len(result) == 2
        assert result['Title'].tolist() == ['Product 1', 'Product 2']


class TestRemoveDuplicates:
    """Test cases untuk fungsi remove_duplicates"""
    
    def test_remove_exact_duplicates(self):
        """Test remove duplikat exact"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 1', 'Product 2'],
            'Price': [100, 100, 200],
            'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        
        result = remove_duplicates(df)
        
        assert len(result) == 2
        assert result['Title'].tolist() == ['Product 1', 'Product 2']
    
    def test_no_duplicates(self):
        """Test ketika tidak ada duplikat"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [100, 200, 300]
        })
        
        result = remove_duplicates(df)
        
        assert len(result) == 3
    
    def test_keep_first_duplicate(self):
        """Test keep first occurrence dari duplikat"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 1'],
            'Price': [100, 100],
            'Timestamp': ['2024-01-01 10:00:00', '2024-01-01 11:00:00']
        })
        
        result = remove_duplicates(df)
        
        assert len(result) == 1
        assert result['Timestamp'].iloc[0] == '2024-01-01 10:00:00'


class TestRemoveNullValues:
    """Test cases untuk fungsi remove_null_values"""
    
    def test_remove_rows_with_null(self):
        """Test remove rows dengan nilai null"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': [100.0, None, 300.0],
            'Rating': [4.5, 3.5, None]
        })
        
        result = remove_null_values(df)
        
        assert len(result) == 1
        assert result['Title'].iloc[0] == 'Product 1'
    
    def test_no_null_values(self):
        """Test ketika tidak ada null values"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Price': [100.0, 200.0]
        })
        
        result = remove_null_values(df)
        
        assert len(result) == 2


class TestTransformData:
    """Test cases untuk fungsi transform_data (integration)"""
    
    def test_transform_complete_pipeline(self):
        """Test complete transformation pipeline"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Unknown Product', 'Product 2', 'Product 2'],
            'Price': ['$100.00', '$200.00', '$150.00', '$150.00'],
            'Rating': ['Rating: ⭐ 4.5 / 5', 'Invalid Rating', 'Rating: ⭐ 3.5 / 5', 'Rating: ⭐ 3.5 / 5'],
            'Colors': ['3 Colors', '5 Colors', '2 Colors', '2 Colors'],
            'Size': ['Size: M', 'Size: L', 'Size: S', 'Size: S'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex', 'Gender: Unisex'],
            'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-03']
        })
        
        result = transform_data(df)
        
        # Should remove Unknown Product, invalid rating, and duplicates
        assert len(result) < len(df)
        assert 'Unknown Product' not in result['Title'].values
        assert result['Price'].dtype == 'float64'
        assert result['Rating'].dtype == 'float64'
        assert result['Colors'].dtype == 'int64'
    
    def test_transform_preserves_valid_data(self):
        """Test transform mempertahankan data valid"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Price': ['$100.00', '$200.00'],
            'Rating': ['Rating: ⭐ 4.5 / 5', 'Rating: ⭐ 3.5 / 5'],
            'Colors': ['3 Colors', '2 Colors'],
            'Size': ['Size: M', 'Size: L'],
            'Gender': ['Gender: Men', 'Gender: Women'],
            'Timestamp': ['2024-01-01', '2024-01-02']
        })
        
        result = transform_data(df)
        
        assert len(result) == 2
        assert result['Title'].tolist() == ['Product 1', 'Product 2']
        assert result['Price'][0] == 1600000.0
        assert result['Rating'][0] == 4.5
    
    def test_transform_handles_edge_cases(self):
        """Test transform dengan berbagai edge cases"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2', 'Product 3'],
            'Price': ['$99.99', '$1,234.56', '$0.01'],
            'Rating': ['Rating: ⭐ 5.0 / 5', 'Rating: ⭐ 0.0 / 5', 'Rating: ⭐ 2.5 / 5'],
            'Colors': ['1 Color', '10 Colors', '3 Colors'],
            'Size': ['Size: XS', 'Size: XXXL', 'Size: M'],
            'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
            'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
        
        result = transform_data(df)
        
        # Should handle all edge cases properly
        assert len(result) >= 1
        assert all(result['Price'] > 0)
        assert all(result['Rating'] >= 0)
        assert all(result['Rating'] <= 5)


# Pytest fixtures
@pytest.fixture
def sample_raw_data():
    """Sample raw data untuk testing"""
    return pd.DataFrame({
        'Title': ['Product 1', 'Unknown Product', 'Product 2'],
        'Price': ['$100.00', '$200.00', '$150.00'],
        'Rating': ['Rating: ⭐ 4.5 / 5', 'Invalid Rating', 'Rating: ⭐ 3.5 / 5'],
        'Colors': ['3 Colors', '5 Colors', '2 Colors'],
        'Size': ['Size: M', 'Size: L', 'Size: S'],
        'Gender': ['Gender: Men', 'Gender: Women', 'Gender: Unisex'],
        'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03']
    })


@pytest.fixture
def sample_clean_data():
    """Sample clean data untuk testing"""
    return pd.DataFrame({
        'Title': ['Product 1', 'Product 2'],
        'Price': [1600000.0, 2400000.0],
        'Rating': [4.5, 3.5],
        'Colors': [3, 2],
        'Size': ['M', 'L'],
        'Gender': ['Men', 'Women'],
        'Timestamp': ['2024-01-01', '2024-01-02']
    })