# Fashion Studio ETL Pipeline

ETL Pipeline untuk scraping, transformasi, dan loading data produk fashion dari website Fashion Studio.

## 🎯 Fitur

- ✅ Web scraping 1000+ produk dari 50 halaman
- ✅ Data cleaning dan transformasi
- ✅ Konversi mata uang (USD → IDR)
- ✅ Export ke multiple repositories (CSV, Google Sheets, PostgreSQL)
- ✅ Unit tests dengan coverage ≥85%
- ✅ Error handling dan logging

## 🛠️ Teknologi

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

- **CSV**: `products.csv` (867 produk valid)
- **Google Sheets**: [Link to your sheet]
- **PostgreSQL**: Database `fashion_products`, table `products`

## 🧪 Test Coverage

Target: ≥80% untuk nilai Advanced
Current: 85%+

## 📝 License

MIT License - see LICENSE file for details

## 👤 Author

[Your Name] - Dicoding Submission
```

---

### **Langkah 3: First Commit**

Di GitHub Desktop:

1. **Summary:** `Initial commit - ETL Pipeline project`
2. **Description:**
```
   - Added extract, transform, load modules
   - Added unit tests with 85%+ coverage
   - Added main orchestrator
   - Added documentation and requirements