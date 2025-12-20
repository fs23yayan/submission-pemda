# Fashion Studio ETL Pipeline

ETL Pipeline for scraping, transformation, and loading of fashion product data from Fashion Studio website.

## 🎯 Features

- ✅ Web scraping 1000+ products from 50 pages
- ✅ Data cleaning and transformation
- ✅ Currency convertion (USD → IDR)
- ✅ Export to multiple repositories (CSV, Google Sheets, PostgreSQL)
- ✅ Unit tests with coverage ≥85%
- ✅ Error handling and logging

## 🛠️ Technology

- Python 3.9+
- BeautifulSoup4 - Web scraping
- Pandas - Data manipulation
- SQLAlchemy - Database connection
- Google Sheets API - Cloud storage
- Pytest - Unit testing

## 📦 Installation
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/fashion-studio-etl-pipeline.git
cd fashion-studio-etl-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Mac/Linux
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

## 🚀 Usage
```bash
# Run full ETL pipeline
python main.py

# Run unit tests
pytest tests/ -v

# Check test coverage
coverage run -m pytest tests/
coverage report
```

## 📊 Data Output

- **CSV**: `products.csv` (867 valid products)
- **Google Sheets**: [[Link to your sheet](https://docs.google.com/spreadsheets/d/19p-1wqJ1fkplCMBVyELMthm1Pvjc-hCAqlf0By9W5cY/edit?usp=drive_link)]
- **PostgreSQL**: Database `fashion_products`, table `products`

## 🧪 Test Coverage

Target: ≥80%
Current: 85%+

## 📝 License

MIT License - see LICENSE file for details

## 👤 Author

Fauzan Suryahadi - Dicoding Submission
