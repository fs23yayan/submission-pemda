"""
Unit tests untuk module load
Testing fungsi-fungsi loading data ke berbagai repositori
"""

import pytest
import pandas as pd
import os
from unittest.mock import Mock, patch, MagicMock, mock_open
from utils.load import (
    save_to_csv,
    save_to_google_sheets,
    save_to_postgresql,
    load_data_to_all_repositories
)


class TestSaveToCSV:
    """Test cases untuk fungsi save_to_csv"""
    
    def test_save_to_csv_success(self, tmp_path):
        """Test save CSV berhasil"""
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Price': [100.0, 200.0]
        })
        
        filename = tmp_path / "test_products.csv"
        result = save_to_csv(df, str(filename))
        
        assert os.path.exists(filename)
        assert result == str(filename)
        
        # Verify content
        df_read = pd.read_csv(filename)
        assert len(df_read) == 2
        assert 'Title' in df_read.columns
    
    def test_save_to_csv_with_default_filename(self, tmp_path):
        """Test save CSV dengan default filename"""
        df = pd.DataFrame({
            'Title': ['Product 1'],
            'Price': [100.0]
        })
        
        os.chdir(tmp_path)
        result = save_to_csv(df)
        
        assert os.path.exists("products.csv")
        assert result == "products.csv"
    
    def test_save_to_csv_empty_dataframe(self, tmp_path):
        """Test save empty DataFrame"""
        df = pd.DataFrame()
        
        filename = tmp_path / "empty.csv"
        result = save_to_csv(df, str(filename))
        
        assert os.path.exists(filename)
    
    def test_save_to_csv_large_dataframe(self, tmp_path):
        """Test save large DataFrame"""
        df = pd.DataFrame({
            'Title': [f'Product {i}' for i in range(1000)],
            'Price': [float(i * 100) for i in range(1000)]
        })
        
        filename = tmp_path / "large.csv"
        result = save_to_csv(df, str(filename))
        
        assert os.path.exists(filename)
        assert os.path.getsize(filename) > 0
    
    def test_save_to_csv_error_handling(self):
        """Test error handling untuk path invalid"""
        df = pd.DataFrame({'A': [1, 2, 3]})
        
        with pytest.raises(Exception):
            save_to_csv(df, "/invalid/path/file.csv")


class TestSaveToGoogleSheets:
    """Test cases untuk fungsi save_to_google_sheets"""
    
    @patch('utils.load.build')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.os.path.exists')
    def test_save_to_google_sheets_success(self, mock_exists, mock_creds, mock_build):
        """Test save ke Google Sheets berhasil"""
        # Mock credentials
        mock_exists.return_value = True
        mock_creds.return_value = Mock()
        
        # Mock service
        mock_service = Mock()
        mock_spreadsheets = Mock()
        mock_service.spreadsheets.return_value = mock_spreadsheets
        
        # Mock get spreadsheet (untuk cek existing sheets)
        mock_get = Mock()
        mock_get.execute.return_value = {
            'sheets': [{'properties': {'title': 'Sheet1'}}]
        }
        mock_spreadsheets.get.return_value = mock_get
        
        # Mock clear
        mock_clear = Mock()
        mock_clear.execute.return_value = {}
        mock_values = Mock()
        mock_values.clear.return_value = mock_clear
        mock_spreadsheets.values.return_value = mock_values
        
        # Mock update
        mock_update = Mock()
        mock_update.execute.return_value = {'updatedCells': 10}
        mock_values.update.return_value = mock_update
        
        mock_build.return_value = mock_service
        
        df = pd.DataFrame({
            'Title': ['Product 1', 'Product 2'],
            'Price': [100.0, 200.0]
        })
        
        result = save_to_google_sheets(
            df,
            spreadsheet_id="test123",
            credentials_file="test_creds.json"
        )
        
        assert "https://docs.google.com/spreadsheets" in result
        assert "test123" in result
    
    @patch('utils.load.os.path.exists')
    def test_save_to_google_sheets_missing_credentials(self, mock_exists):
        """Test error ketika credentials file tidak ada"""
        mock_exists.return_value = False
        
        df = pd.DataFrame({'A': [1, 2]})
        
        with pytest.raises(Exception, match="Credentials file not found"):
            save_to_google_sheets(df, "test123", "missing.json")
    
    @patch('utils.load.build')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.os.path.exists')
    def test_save_to_google_sheets_create_new_sheet(self, mock_exists, mock_creds, mock_build):
        """Test create new sheet jika tidak ada"""
        mock_exists.return_value = True
        mock_creds.return_value = Mock()
        
        mock_service = Mock()
        mock_spreadsheets = Mock()
        mock_service.spreadsheets.return_value = mock_spreadsheets
        
        # Mock get - sheet Products tidak ada
        mock_get = Mock()
        mock_get.execute.return_value = {
            'sheets': [{'properties': {'title': 'Sheet1'}}]
        }
        mock_spreadsheets.get.return_value = mock_get
        
        # Mock batchUpdate untuk create sheet
        mock_batch = Mock()
        mock_batch.execute.return_value = {}
        mock_spreadsheets.batchUpdate.return_value = mock_batch
        
        # Mock clear dan update
        mock_values = Mock()
        mock_clear = Mock()
        mock_clear.execute.return_value = {}
        mock_values.clear.return_value = mock_clear
        
        mock_update = Mock()
        mock_update.execute.return_value = {'updatedCells': 10}
        mock_values.update.return_value = mock_update
        
        mock_spreadsheets.values.return_value = mock_values
        mock_build.return_value = mock_service
        
        df = pd.DataFrame({'A': [1, 2]})
        
        result = save_to_google_sheets(df, "test123", "test.json", "Products")
        
        assert "https://docs.google.com/spreadsheets" in result
    
    @patch('utils.load.build')
    @patch('utils.load.service_account.Credentials.from_service_account_file')
    @patch('utils.load.os.path.exists')
    def test_save_to_google_sheets_error_handling(self, mock_exists, mock_creds, mock_build):
        """Test error handling saat API call gagal"""
        mock_exists.return_value = True
        mock_creds.return_value = Mock()
        
        mock_service = Mock()
        mock_service.spreadsheets.side_effect = Exception("API Error")
        mock_build.return_value = mock_service
        
        df = pd.DataFrame({'A': [1, 2]})
        
        with pytest.raises(Exception):
            save_to_google_sheets(df, "test123", "test.json")


class TestSaveToPostgreSQL:
    """Test cases untuk fungsi save_to_postgresql"""
    
    def test_save_to_postgresql_missing_config(self):
        """Test error ketika config tidak lengkap"""
        df = pd.DataFrame({'A': [1, 2]})
        
        incomplete_config = {
            'host': 'localhost',
            'port': 5432
            # missing database, user, password
        }
        
        with pytest.raises(Exception, match="Missing required config"):
            save_to_postgresql(df, incomplete_config)
    
    @patch('utils.load.create_engine')
    def test_save_to_postgresql_connection_error(self, mock_create_engine):
        """Test error handling saat connection gagal"""
        mock_engine = Mock()
        mock_engine.connect.side_effect = Exception("Connection failed")
        mock_create_engine.return_value = mock_engine
        
        df = pd.DataFrame({'A': [1, 2]})
        
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
        
        with pytest.raises(Exception):
            save_to_postgresql(df, db_config)


class TestLoadDataToAllRepositories:
    """Test cases untuk fungsi load_data_to_all_repositories (integration)"""
    
    @patch('utils.load.save_to_postgresql')
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_csv')
    def test_load_all_repositories_success(self, mock_csv, mock_sheets, mock_pg):
        """Test load ke semua repositories berhasil"""
        mock_csv.return_value = "products.csv"
        mock_sheets.return_value = "https://sheets.google.com/test"
        mock_pg.return_value = "localhost:5432/testdb"
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(
            df=df,
            csv_filename="test.csv",
            google_sheets_id="test123",
            postgresql_config={'host': 'localhost', 'port': 5432, 
                             'database': 'test', 'user': 'user', 'password': 'pass'}
        )
        
        assert results['csv']['status'] == 'success'
        assert results['google_sheets']['status'] == 'success'
        assert results['postgresql']['status'] == 'success'
    
    @patch('utils.load.save_to_csv')
    def test_load_csv_only(self, mock_csv):
        """Test load hanya ke CSV"""
        mock_csv.return_value = "products.csv"
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(
            df=df,
            google_sheets_id=None,
            postgresql_config=None
        )
        
        assert results['csv']['status'] == 'success'
        assert results['google_sheets']['status'] == 'skipped'
        assert results['postgresql']['status'] == 'skipped'
    
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_csv')
    def test_load_with_google_sheets_failure(self, mock_csv, mock_sheets):
        """Test ketika Google Sheets gagal tapi CSV berhasil"""
        mock_csv.return_value = "products.csv"
        mock_sheets.side_effect = Exception("Sheets API error")
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(
            df=df,
            google_sheets_id="test123"
        )
        
        assert results['csv']['status'] == 'success'
        assert results['google_sheets']['status'] == 'failed'
    
    @patch('utils.load.save_to_csv')
    def test_load_with_csv_failure(self, mock_csv):
        """Test ketika CSV gagal"""
        mock_csv.side_effect = Exception("Disk full")
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(df=df)
        
        assert results['csv']['status'] == 'failed'
        assert 'Disk full' in results['csv']['info']
    
    @patch('utils.load.save_to_postgresql')
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_csv')
    def test_load_partial_success(self, mock_csv, mock_sheets, mock_pg):
        """Test ketika sebagian repository berhasil, sebagian gagal"""
        mock_csv.return_value = "products.csv"
        mock_sheets.side_effect = Exception("API error")
        mock_pg.return_value = "localhost:5432/testdb"
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(
            df=df,
            google_sheets_id="test123",
            postgresql_config={'host': 'localhost', 'port': 5432,
                             'database': 'test', 'user': 'user', 'password': 'pass'}
        )
        
        assert results['csv']['status'] == 'success'
        assert results['google_sheets']['status'] == 'failed'
        assert results['postgresql']['status'] == 'success'
    
    @patch('utils.load.save_to_google_sheets')
    @patch('utils.load.save_to_csv')
    def test_load_with_custom_filenames(self, mock_csv, mock_sheets):
        """Test load dengan custom filenames"""
        mock_csv.return_value = "custom.csv"
        mock_sheets.return_value = "https://sheets.google.com/test"
        
        df = pd.DataFrame({'A': [1, 2]})
        
        results = load_data_to_all_repositories(
            df=df,
            csv_filename="custom.csv",
            google_sheets_id="test123",
            google_credentials_file="custom_creds.json"
        )
        
        assert results['csv']['status'] == 'success'
        mock_csv.assert_called_once()
        mock_sheets.assert_called_once()


class TestErrorHandling:
    """Test cases untuk error handling"""
    
    def test_save_csv_invalid_dataframe(self):
        """Test save CSV dengan DataFrame invalid"""
        with pytest.raises(Exception):
            save_to_csv(None, "test.csv")
    
    @patch('utils.load.create_engine')
    def test_postgresql_invalid_credentials(self, mock_create_engine):
        """Test PostgreSQL dengan credentials invalid"""
        mock_engine = Mock()
        mock_engine.connect.side_effect = Exception("Authentication failed")
        mock_create_engine.return_value = mock_engine
        
        df = pd.DataFrame({'A': [1, 2]})
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test',
            'user': 'wrong',
            'password': 'wrong'
        }
        
        with pytest.raises(Exception):
            save_to_postgresql(df, db_config)


# Pytest fixtures
@pytest.fixture
def sample_dataframe():
    """Sample DataFrame untuk testing"""
    return pd.DataFrame({
        'Title': ['Product 1', 'Product 2', 'Product 3'],
        'Price': [100.0, 200.0, 300.0],
        'Rating': [4.5, 3.5, 4.0],
        'Colors': [3, 2, 5],
        'Size': ['M', 'L', 'XL'],
        'Gender': ['Men', 'Women', 'Unisex'],
        'Timestamp': ['2024-01-01', '2024-01-02', '2024-01-03']
    })


@pytest.fixture
def mock_db_config():
    """Mock database config untuk testing"""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_password'
    }