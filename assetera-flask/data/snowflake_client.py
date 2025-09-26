"""
Snowflake data access layer for AssetEra backtesting
Replaces Yahoo Finance API calls with Snowflake database queries
"""

import pandas as pd
import snowflake.connector
from datetime import date, datetime
from typing import List, Dict, Optional
import os
from flask import current_app

class SnowflakeClient:
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Snowflake"""
        print(f"This is configuration {current_app.config} endl")
        print(current_app.config['SNOWFLAKE_USER'])
        print(current_app.config['SNOWFLAKE_PASSWORD'])
        print(current_app.config['SNOWFLAKE_ACCOUNT'])
        try:
            self.connection = snowflake.connector.connect(
                user=current_app.config['SNOWFLAKE_USER'],
                password=current_app.config['SNOWFLAKE_PASSWORD'],
                account=current_app.config['SNOWFLAKE_ACCOUNT'],
                warehouse=current_app.config['SNOWFLAKE_WAREHOUSE'],
                database=current_app.config['SNOWFLAKE_DATABASE'],
                schema=current_app.config['SNOWFLAKE_SCHEMA'],
                role=current_app.config['SNOWFLAKE_ROLE']
            )
            print("✓ Connected to Snowflake successfully")
        except Exception as e:
            print(f"✗ Failed to connect to Snowflake: {str(e)}")
            raise
    
    def _ensure_connection(self):
        """Ensure connection is active"""
        if not self.connection or self.connection.is_closed():
            self._connect()
    
    def fetch_price_data(self, tickers: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fetch historical price data from Snowflake DAILY_PRICES table
        Returns DataFrame with dates as index and tickers as columns
        """
        self._ensure_connection()
        
        # Convert tickers list to SQL IN clause
        ticker_list = "', '".join(tickers)
        
        query = f"""
        SELECT DATE, TICKER, PRICE
        FROM DAILY_PRICES
        WHERE TICKER IN ('{ticker_list}')
        AND DATE >= '{start_date}'
        AND DATE <= '{end_date}'
        ORDER BY DATE, TICKER
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Fetch all results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Convert to DataFrame
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            
            if df.empty:
                print(f"No data found for tickers {tickers} between {start_date} and {end_date}")
                return pd.DataFrame()
            
            # Pivot to get tickers as columns
            price_df = df.pivot(index='DATE', columns='TICKER', values='PRICE')
            
            # Ensure index is datetime
            price_df.index = pd.to_datetime(price_df.index)
            price_df.index.name = 'Date'
            
            # Forward fill missing values
            price_df = price_df.ffill()
            
            print(f"✓ Fetched {len(price_df)} rows for {len(price_df.columns)} tickers from Snowflake")
            return price_df
            
        except Exception as e:
            print(f"✗ Error fetching price data from Snowflake: {str(e)}")
            raise
    
    def get_fund_allocations(self, fund_id: str) -> Dict[str, float]:
        """Get fund allocation weights from Snowflake"""
        self._ensure_connection()
        
        query = f"""
        SELECT TICKER, WEIGHT
        FROM FUND_ALLOCATIONS
        WHERE FUND_ID = '{fund_id}'
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return {ticker: float(weight) for ticker, weight in results}
            
        except Exception as e:
            print(f"✗ Error fetching fund allocations: {str(e)}")
            return {}
    
    def get_available_tickers(self) -> List[str]:
        """Get list of all available tickers in the database"""
        self._ensure_connection()
        
        query = "SELECT DISTINCT TICKER FROM DAILY_PRICES ORDER BY TICKER"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return [row[0] for row in results]
            
        except Exception as e:
            print(f"✗ Error fetching available tickers: {str(e)}")
            return []
    
    def get_date_range(self) -> tuple:
        """Get the available date range in the database"""
        self._ensure_connection()
        
        query = "SELECT MIN(DATE), MAX(DATE) FROM DAILY_PRICES"
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] and result[1]:
                return result[0], result[1]
            else:
                return None, None
                
        except Exception as e:
            print(f"✗ Error fetching date range: {str(e)}")
            return None, None
    
    def close(self):
        """Close the Snowflake connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            print("✓ Snowflake connection closed")

# Global instance
snowflake_client = None

def get_snowflake_client() -> SnowflakeClient:
    """Get or create Snowflake client instance"""
    global snowflake_client
    if snowflake_client is None:
        snowflake_client = SnowflakeClient()
    return snowflake_client
