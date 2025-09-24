#!/usr/bin/env python3
"""
AssetEra Fund Backtester - Snowflake Data Loader
This script downloads historical price data and loads it into Snowflake
Run this locally to populate your Snowflake database
"""

import pandas as pd
import numpy as np
import yfinance as yf
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import warnings
from datetime import datetime, date, timedelta
from typing import List, Dict
import time
import requests
import json

warnings.filterwarnings('ignore')

# =========================
# Snowflake Connection Config
# =========================
SNOWFLAKE_CONFIG = {
    'user': 'SAIKRISHNA_DEV',           # Replace with your Snowflake username
    'password': 'Saikrishna*1234',       # Replace with your Snowflake password
    'account': 'RMCVKYD-LK80402',         # Replace with your account identifier (e.g., abc123.us-east-1)
    'warehouse': 'COMPUTE_WH',         # Use your warehouse name
    'database': 'ASSETERA_DEV',         # We'll create this
    'schema': 'PUBLIC',                # Default schema
    'role': 'ASSETERA_DEV'             # Use ACCOUNTADMIN or SYSADMIN
}

# Fund definitions (same as your app)
FUNDS = {
    "F1": {
        "name": "Fund 1 — Core Income (low risk)",
        "allocations": {
            "LQD": 0.50, "IEF": 0.20, "GLD": 0.10, "VEA": 0.10,
            "MSFT": 0.02, "APH": 0.02, "GWW": 0.02, "PH": 0.02, "BSX": 0.02,
        }
    },
    "F2": {
        "name": "Fund 2 — Pro Core (~12% in 10y)",
        "allocations": {
            "LQD": 0.30, "IEF": 0.10, "GLD": 0.08, "VEA": 0.12, "SPY": 0.12,
            "MSFT": 0.03, "APH": 0.03, "GWW": 0.03, "PH": 0.03, "BSX": 0.03, "ETN": 0.03,
            "EME": 0.025, "PWR": 0.025, "FAST": 0.025, "BWXT": 0.025,
        }
    },
    "F3": {
        "name": "Fund 3 — Pro Growth 17 (RS≈3.10)",
        "allocations": {
            "LQD": 0.098, "IEF": 0.098, "SPY": 0.060, "VEA": 0.120, "GLD": 0.112,
            "NVDA": 0.025, "AVGO": 0.025, "MSFT": 0.025, "KLAC": 0.025,
            "CDNS": 0.025, "ETN": 0.025, "PH": 0.025, "HEI": 0.025,
            "EME": 0.025, "PWR": 0.025, "FAST": 0.025, "BWXT": 0.025,
            "IDCC": 0.0302857143, "RDNT": 0.0302857143, "DY": 0.0302857143,
            "GPI": 0.0302857143, "ACLS": 0.0302857143, "TTMI": 0.0302857143, "AGM": 0.0302857143,
        }
    },
    "F4": {
        "name": "Fund 4 — Redeem Surge 31 (max RS for >30%)",
        "allocations": {
            "NVDA": 0.24, "AVGO": 0.12, "KLAC": 0.06, "CDNS": 0.06,
            "IDCC": 0.0125, "RDNT": 0.0125, "ACLS": 0.0125, "GPI": 0.0125,
            "VWO": 0.32, "GLD": 0.15,
        }
    },
    "F5": {
        "name": "Fund 5 — Bridge Growth 26 (between F3 & F4)",
        "allocations": {
            "NVDA": 0.10, "AVGO": 0.07, "KLAC": 0.06, "CDNS": 0.05, "MSFT": 0.05, "ETN": 0.05,
            "EME": 0.04, "PWR": 0.04, "FAST": 0.025, "BWXT": 0.025,
            "IDCC": 0.06, "RDNT": 0.06, "ACLS": 0.06, "GPI": 0.05, "AGM": 0.05, "TTMI": 0.05,
            "VEA": 0.08, "GLD": 0.04, "VWO": 0.02, "LQD": 0.012, "IEF": 0.008,
        }
    }
}

BENCHMARKS = {
    "SPY": {"name": "S&P 500 (SPY)", "ticker": "SPY"},
    "GLD": {"name": "Gold (GLD)", "ticker": "GLD"},
    "VEA": {"name": "Developed ex-US (VEA)", "ticker": "VEA"},
    "VWO": {"name": "Emerging (VWO)", "ticker": "VWO"},
    "IEF": {"name": "UST 7–10y (IEF)", "ticker": "IEF"},
    "LQD": {"name": "US IG Corp (LQD)", "ticker": "LQD"}
}

def get_all_tickers() -> List[str]:
    """Get all unique tickers from funds and benchmarks"""
    tickers = set()
    
    # Add fund tickers
    for fund_data in FUNDS.values():
        tickers.update(fund_data["allocations"].keys())
    
    # Add benchmark tickers
    for bench_data in BENCHMARKS.values():
        tickers.add(bench_data["ticker"])
    
    return sorted(list(tickers))

def manual_yahoo_fetch(symbol: str, start_date: date, end_date: date) -> pd.Series:
    """Fetch data using direct Yahoo Finance API (your working function)"""
    
    start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    end_timestamp = int(datetime.combine(end_date, datetime.min.time()).timestamp())
    
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        'period1': start_timestamp,
        'period2': end_timestamp,
        'interval': '1d',
        'events': 'history'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'chart' in data and data['chart']['result'] and len(data['chart']['result']) > 0:
                result = data['chart']['result'][0]
                
                if 'timestamp' in result and 'indicators' in result:
                    timestamps = result['timestamp']
                    indicators = result['indicators']
                    
                    if 'quote' in indicators and len(indicators['quote']) > 0:
                        quote = indicators['quote'][0]
                        
                        # Get adjusted close if available
                        if 'adjclose' in indicators and len(indicators['adjclose']) > 0:
                            prices = indicators['adjclose'][0]['adjclose']
                        else:
                            prices = quote.get('close', [])
                        
                        if timestamps and prices:
                            dates = [datetime.fromtimestamp(ts) for ts in timestamps]
                            series = pd.Series(prices, index=pd.DatetimeIndex(dates), name=symbol)
                            return series.dropna()
        
        print(f"Failed to fetch {symbol}: Status {response.status_code}")
        
    except Exception as e:
        print(f"Error fetching {symbol}: {str(e)}")
    
    return pd.Series(dtype=float, name=symbol)

def fetch_all_price_data(tickers: List[str], start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch price data for all tickers"""
    
    print(f"\nFetching price data for {len(tickers)} tickers...")
    print(f"Date range: {start_date} to {end_date}")
    
    all_data = {}
    failed_tickers = []
    
    for i, ticker in enumerate(tickers):
        print(f"Fetching {ticker} ({i+1}/{len(tickers)})...")
        
        try:
            series = manual_yahoo_fetch(ticker, start_date, end_date)
            
            if not series.empty:
                all_data[ticker] = series
                print(f"  ✓ Got {len(series)} data points")
            else:
                failed_tickers.append(ticker)
                print(f"  ✗ No data returned")
            
            time.sleep(0.1)  # Rate limiting
            
        except Exception as e:
            failed_tickers.append(ticker)
            print(f"  ✗ Error: {str(e)}")
    
    if failed_tickers:
        print(f"\nFailed tickers: {failed_tickers}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        df.index.name = "DATE"
        df = df.sort_index().ffill()
        print(f"\n✓ Successfully fetched data: {len(df)} rows × {len(df.columns)} columns")
        return df
    else:
        raise Exception("No price data was successfully fetched")

def connect_to_snowflake():
    """Connect to Snowflake"""
    print("\nConnecting to Snowflake...")
    
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✓ Connected to Snowflake successfully")
        return conn
    except Exception as e:
        print(f"✗ Failed to connect to Snowflake: {str(e)}")
        print("\nPlease check your connection settings in SNOWFLAKE_CONFIG")
        raise

def create_database_schema(conn):
    """Create database and tables"""
    print("\nCreating database and tables...")
    
    cursor = conn.cursor()
    
    try:
        # Create database
        # cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        
        # Create price data table
        cursor.execute("""
        CREATE OR REPLACE TABLE DAILY_PRICES (
            DATE DATE,
            TICKER VARCHAR(10),
            PRICE DECIMAL(12,4),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        # Create fund allocations table
        cursor.execute("""
        CREATE OR REPLACE TABLE FUND_ALLOCATIONS (
            FUND_ID VARCHAR(10),
            FUND_NAME VARCHAR(100),
            TICKER VARCHAR(10),
            WEIGHT DECIMAL(8,6),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        # Create benchmarks table
        cursor.execute("""
        CREATE OR REPLACE TABLE BENCHMARKS (
            BENCHMARK_ID VARCHAR(20),
            BENCHMARK_NAME VARCHAR(100),
            TICKER VARCHAR(10),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        
        print("✓ Database and tables created successfully")
        
    except Exception as e:
        print(f"✗ Error creating database schema: {str(e)}")
        raise
    finally:
        cursor.close()

def load_price_data(conn, price_df: pd.DataFrame):
    """Load price data into Snowflake"""
    print(f"\nLoading price data ({len(price_df)} rows)...")
    
    # Convert DataFrame to long format with proper date handling
    
    price_long = price_df.reset_index().melt(
        id_vars=['DATE'], 
        var_name='TICKER', 
        value_name='PRICE'
    )
    
    # Ensure DATE column is properly formatted as date
    price_long['DATE'] = pd.to_datetime(price_long['DATE']).dt.date
    
    # Remove any NaN values
    price_long = price_long.dropna()
    
    print(f"Prepared {len(price_long)} price records for loading...")
    
    try:
        # Use write_pandas for efficient loading
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            price_long, 
            'DAILY_PRICES',
            auto_create_table=False,
            overwrite=True
        )
        
        if success:
            print(f"✓ Loaded {nrows} price records in {nchunks} chunks")
        else:
            raise Exception("write_pandas returned False")
            
    except Exception as e:
        print(f"✗ Error loading price data: {str(e)}")
        raise

def load_fund_data(conn):
    """Load fund allocation data into Snowflake"""
    print("\nLoading fund allocation data...")
    
    fund_records = []
    for fund_id, fund_data in FUNDS.items():
        for ticker, weight in fund_data["allocations"].items():
            fund_records.append({
                'FUND_ID': fund_id,
                'FUND_NAME': fund_data["name"],
                'TICKER': ticker,
                'WEIGHT': weight
            })
    
    fund_df = pd.DataFrame(fund_records)
    
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            fund_df, 
            'FUND_ALLOCATIONS',
            auto_create_table=False,
            overwrite=True
        )
        
        if success:
            print(f"✓ Loaded {nrows} fund allocation records")
        else:
            raise Exception("write_pandas returned False")
            
    except Exception as e:
        print(f"✗ Error loading fund data: {str(e)}")
        raise

def load_benchmark_data(conn):
    """Load benchmark data into Snowflake"""
    print("\nLoading benchmark data...")
    
    benchmark_records = []
    for bench_id, bench_data in BENCHMARKS.items():
        benchmark_records.append({
            'BENCHMARK_ID': bench_id,
            'BENCHMARK_NAME': bench_data["name"],
            'TICKER': bench_data["ticker"]
        })
    
    benchmark_df = pd.DataFrame(benchmark_records)
    
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn, 
            benchmark_df, 
            'BENCHMARKS',
            auto_create_table=False,
            overwrite=True
        )
        
        if success:
            print(f"✓ Loaded {nrows} benchmark records")
        else:
            raise Exception("write_pandas returned False")
            
    except Exception as e:
        print(f"✗ Error loading benchmark data: {str(e)}")
        raise

def verify_data(conn):
    """Verify that data was loaded correctly"""
    print("\nVerifying loaded data...")
    
    cursor = conn.cursor()
    
    try:
        # Check price data
        cursor.execute("SELECT COUNT(*) FROM DAILY_PRICES")
        price_count = cursor.fetchone()[0]
        print(f"✓ DAILY_PRICES: {price_count:,} records")
        
        # Check fund data
        cursor.execute("SELECT COUNT(*) FROM FUND_ALLOCATIONS")
        fund_count = cursor.fetchone()[0]
        print(f"✓ FUND_ALLOCATIONS: {fund_count} records")
        
        # Check benchmark data
        cursor.execute("SELECT COUNT(*) FROM BENCHMARKS")
        benchmark_count = cursor.fetchone()[0]
        print(f"✓ BENCHMARKS: {benchmark_count} records")
        
        # Show date range
        cursor.execute("SELECT MIN(DATE), MAX(DATE) FROM DAILY_PRICES")
        min_date, max_date = cursor.fetchone()
        print(f"✓ Price data date range: {min_date} to {max_date}")
        
        # Show sample data
        cursor.execute("""
            SELECT TICKER, COUNT(*) as DAYS 
            FROM DAILY_PRICES 
            GROUP BY TICKER 
            ORDER BY TICKER 
            LIMIT 10
        """)
        print(f"\nSample ticker data:")
        for ticker, days in cursor.fetchall():
            print(f"  {ticker}: {days} days")
            
    except Exception as e:
        print(f"✗ Error verifying data: {str(e)}")
        raise
    finally:
        cursor.close()

def main():
    """Main execution function"""
    print("AssetEra Fund Backtester - Data Loader")
    print("=" * 50)
    
    # Configuration
    start_date = date(2015, 1, 1)  # 10 years of data
    end_date = date.today()
    
    print(f"Data range: {start_date} to {end_date}")
    
    # Step 1: Get all required tickers
    tickers = get_all_tickers()
    print(f"Total tickers to fetch: {len(tickers)}")
    print(f"Tickers: {', '.join(tickers)}")
    
    # Step 2: Fetch price data
    price_df = fetch_all_price_data(tickers, start_date, end_date)
    
    # Step 3: Connect to Snowflake
    conn = connect_to_snowflake()
    
    try:
        # Step 4: Create database schema
        create_database_schema(conn)
        
        # Step 5: Load data
        load_price_data(conn, price_df)
        load_fund_data(conn)
        load_benchmark_data(conn)
        
        # Step 6: Verify data
        verify_data(conn)
        
        print("\n" + "=" * 50)
        print("✓ Data loading completed successfully!")
        print("\nYou can now use the Snowflake-based Streamlit app.")
        print("The following tables are available:")
        print("  - DAILY_PRICES: Historical price data")
        print("  - FUND_ALLOCATIONS: Fund composition data") 
        print("  - BENCHMARKS: Benchmark definitions")
        
    finally:
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    # Before running, update SNOWFLAKE_CONFIG with your credentials
    print("IMPORTANT: Update SNOWFLAKE_CONFIG with your Snowflake credentials before running!")
    print("Press Enter to continue or Ctrl+C to exit...")
    input()
    
    main()