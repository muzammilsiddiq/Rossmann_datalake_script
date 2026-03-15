# Rossmann Store Sales - Star Schema Data Warehouse

A complete ETL pipeline that transforms Rossmann Store Sales raw data into a dimensional data warehouse using a Star Schema design, with data stored in Supabase (PostgreSQL).

## 📋 Table of Contents
- [Project Overview](#-project-overview)
- [Schema Design](#-schema-design)
- [Features](#-features)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Supabase Setup](#-supabase-setup)
- [Configuration](#-configuration)
- [Running the ETL](#-running-the-etl)
- [Sample Queries](#-sample-queries)
- [Troubleshooting](#-troubleshooting)
- [Project Structure](#-project-structure)
- [License](#-license)

## 📋 Project Overview

This project implements a data warehouse solution for the Rossmann Store Sales dataset. It transforms flat CSV files into a structured star schema with fact and dimension tables, making it optimized for analytical queries and business intelligence.

## 📊 Schema Design

The data warehouse follows a star schema with the following structure:

### Table Details

#### Fact Table
| Column | Type | Description |
|--------|------|-------------|
| sales_id | INTEGER (PK) | Auto-generated primary key |
| store_id | INTEGER (FK) | References dim_store |
| dim_competition | INTEGER (FK) | References dim_competition |
| dim_promotion | INTEGER (FK) | References dim_promotion |
| dim_time | INTEGER (FK) | References dim_time |
| turnover | FLOAT8 | Daily sales amount |
| nr_customers | INTEGER | Number of customers |
| turnover_per_customer | FLOAT8 | Calculated metric |

#### Dimension Tables

**dim_store**
| Column | Type | Description |
|--------|------|-------------|
| store_id | INTEGER (PK) | Store identifier |
| store_type | TEXT | a, b, c, d |
| assortment | TEXT | Basic, Extra, Extended |

**dim_competition**
| Column | Type | Description |
|--------|------|-------------|
| competition_id | INTEGER (PK) | Auto-generated |
| distance | INTEGER | Distance to competitor (meters) |
| open | INTEGER | 0/1 flag if competition exists |
| open_year | INTEGER | Year competition opened |
| open_month | INTEGER | Month competition opened |

**dim_promotion**
| Column | Type | Description |
|--------|------|-------------|
| promotion_id | INTEGER (PK) | Auto-generated |
| promotion | INTEGER | 0/1 if promo2 is active |
| promotion_year | INTEGER | Year promo2 started |
| promotion_interval | TEXT | Months when promo runs |

**dim_time**
| Column | Type | Description |
|--------|------|-------------|
| time_id | INTEGER (PK) | Auto-generated |
| full_date | DATE | Calendar date |
| school_holiday | INTEGER | 0/1 if school holiday |
| state_holiday | INTEGER | 0=None, 1=Public, 2=Easter, 3=Christmas |
| month | INTEGER | Month (1-12) |
| year | INTEGER | Year |

## 🚀 Features

- **Complete ETL Pipeline**: Extract, transform, and load data automatically
- **Star Schema Design**: Optimized for analytical queries
- **Supabase Integration**: Cloud-based PostgreSQL database
- **Batch Processing**: Handles large datasets (500 records per batch)
- **Data Validation**: Automatic verification after loading
- **Secure Credentials**: Environment variable management
- **Error Handling**: Comprehensive error logging and recovery
- **Auto-generated IDs**: Database handles primary key generation

## 📦 Prerequisites

- Python 3.8 or higher
- Supabase account (free tier works)
- Rossmann Store Sales CSV files:
  - `store.csv`
  - `train.csv`
- Git (optional)

## ⚡ Quick Start

```bash
# 1. Clone or download this repository
git clone https://github.com/yourusername/rossmann-star-schema.git
cd rossmann-star-schema

# 2. Install dependencies
pip install pandas numpy supabase python-dotenv

# 3. Set up Supabase and get credentials
#    - Create project at https://supabase.com
#    - Get URL and anon key from Project Settings → API

# 4. Create .env file with your credentials
echo "SUPABASE_URL=your_url_here" > .env
echo "SUPABASE_KEY=your_key_here" >> .env

## 🔧 Installation

### 1. Clone or Download the Repository
```bash
git clone https://github.com/yourusername/rossmann-star-schema.git
cd rossmann-star-schema
```

### 2. Create and Activate Virtual Environment (Optional but Recommended)

**On macOS/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install Dependencies

Create a `requirements.txt` file with the following content:
```txt
pandas>=1.3.0
numpy>=1.21.0
supabase>=1.0.0
python-dotenv>=0.19.0
```

Then install the dependencies:
```bash
pip install -r requirements.txt
```

### 4. Alternative: Install Dependencies Directly
If you prefer to install without a requirements file:
```bash
pip install pandas numpy supabase python-dotenv
```

### 5. Verify Installation
Run this command to verify all packages are installed correctly:
```bash
python -c "import pandas, numpy, supabase, dotenv; print('All packages installed successfully!')"
```

Expected output:
```
All packages installed successfully!
```

### 6. Project Files Overview

After installation, your project directory should contain these files:
```
rossmann-star-schema/
│
├── etl_pipeline.py          # Main ETL script (you need to create this)
├── supabase_schema.sql       # Database schema (you need to create this)
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (create this)
├── .env.example              # Template for environment variables
├── .gitignore                # Git ignore rules
└── README.md                 # This file
```

### 7. Common Installation Issues and Solutions

| Issue | Solution |
|-------|----------|
| `pip: command not found` | Install pip: `python -m ensurepip --upgrade` |
| Permission denied | Use `pip install --user` or create virtual environment |
| SSL certificate errors | Upgrade pip: `pip install --upgrade pip` |
| `supabase` not found | Try: `pip install supabase-py` |

### 8. Docker Installation (Alternative)

If you prefer using Docker, create a `Dockerfile`:
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "etl_pipeline.py"]
```

Build and run with Docker:
```bash
docker build -t rossmann-etl .
docker run --env-file .env rossmann-etl
```

# 5. Update CSV path in etl_pipeline.py
#    - Change data_path to your CSV location

# 6. Run the ETL pipeline
python etl_pipeline.py
