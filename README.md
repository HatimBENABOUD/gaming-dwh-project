# Steam Gaming DWH

An end-to-end gaming data warehouse project based on Steam data. The project demonstrates SQL ingestion, medallion architecture, PySpark transformations, MySQL modeling, and Power BI reporting.

## Architecture

- **Bronze:** Raw MySQL tables and Parquet data
- **Silver:** Cleaned and standardized tables
- **Gold:** Star schema with fact and dimension tables
- **Reporting:** Power BI dashboard

## Project structure

```text
.
|-- data/raw/                 # Source CSV files (large datasets are gitignored)
|-- data/lakehouse/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- scripts/
|   |-- ingest_bronze.py      # Chunked CSV ingestion into MySQL
|   `-- pyspark_medallion.py  # Bronze, Silver, and Gold Spark pipeline
|-- sql/
|   |-- 01_bronze_layer.sql
|   |-- 02_silver_layer.sql
|   `-- 03_gold_layer.sql
`-- dashbord/image.png         # Power BI dashboard preview
```

## Data pipeline

1. Load source CSV files into MySQL bronze tables.
2. Clean and normalize bronze records into silver tables.
3. Build the gold star schema for analytics.
4. Import modeled tables or Parquet files into Power BI.

The large recommendation and user datasets are intentionally not committed. Download or place the source CSV files in `data/raw/` before running the pipeline.

## Technologies

- Python
- PySpark
- pandas
- SQLAlchemy
- MySQL
- Apache Parquet
- Power BI

## Running the project

Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create the MySQL database and bronze tables:

```powershell
mysql -u root -p < sql\01_bronze_layer.sql
```

Run the MySQL ingestion:

```powershell
python scripts\ingest_bronze.py
```

Run the Silver SQL transformations:

```powershell
mysql -u root -p steam_dwh < sql\02_silver_layer.sql
```

Run the PySpark lakehouse pipeline:

```powershell
python scripts\pyspark_medallion.py
```
