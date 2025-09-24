import requests
import json
import time
import pandas as pd
from datetime import datetime, date
from typing import List, Dict

class YahooFinanceService:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
        }
    
    def manual_yahoo_fetch(self, symbol: str, start_date: date, end_date: date) -> pd.Series:
        """Fetch data for a single symbol using Yahoo Finance API directly"""
        
        # Convert dates to timestamps
        start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
        end_timestamp = int(datetime.combine(end_date, datetime.min.time()).timestamp())
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            'period1': start_timestamp,
            'period2': end_timestamp,
            'interval': '1d',
            'events': 'history'
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'chart' in data and data['chart']['result'] and len(data['chart']['result']) > 0:
                    result = data['chart']['result'][0]
                    
                    if 'timestamp' in result and 'indicators' in result:
                        timestamps = result['timestamp']
                        indicators = result['indicators']
                        
                        if 'quote' in indicators and len(indicators['quote']) > 0:
                            quote = indicators['quote'][0]
                            
                            # Get adjusted close if available, otherwise use close
                            if 'adjclose' in indicators and len(indicators['adjclose']) > 0:
                                prices = indicators['adjclose'][0]['adjclose']
                            else:
                                prices = quote.get('close', [])
                            
                            if timestamps and prices:
                                # Create datetime index
                                dates = [datetime.fromtimestamp(ts) for ts in timestamps]
                                
                                # Create series and clean it
                                series = pd.Series(prices, index=pd.DatetimeIndex(dates), name=symbol)
                                series = series.dropna()
                                
                                return series
            
            print(f"Failed to fetch {symbol}: Status {response.status_code}")
            
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")
        
        return pd.Series(dtype=float, name=symbol)

    def fetch_prices(self, tickers: List[str], start: date, end: date) -> Dict:
        """
        Fetch prices using manual Yahoo Finance API calls
        Returns a dictionary with data and status information
        """
        
        tickers = list(tickers)
        all_data = {}
        failed_tickers = []
        progress_data = []
        
        for i, ticker in enumerate(tickers):
            progress_data.append({
                'ticker': ticker,
                'progress': (i + 1) / len(tickers),
                'status': 'fetching'
            })
            
            try:
                series = self.manual_yahoo_fetch(ticker, start, end)
                
                if not series.empty:
                    all_data[ticker] = series
                    print(f"✓ {ticker}: {len(series)} data points")
                else:
                    failed_tickers.append(ticker)
                    print(f"✗ {ticker}: No data returned")
                
                # Rate limiting - be nice to Yahoo Finance
                time.sleep(0.1)
                
            except Exception as e:
                failed_tickers.append(ticker)
                print(f"✗ {ticker}: Error - {str(e)}")
        
        # Prepare result
        result = {
            'success': bool(all_data),
            'failed_tickers': failed_tickers,
            'data': pd.DataFrame(),
            'message': ''
        }
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.index.name = "Date"
            df = df.sort_index()
            df = df.ffill()  # Forward fill missing values
            
            result['data'] = df
            result['message'] = f"✅ Successfully fetched data for {len(df.columns)} tickers ({len(df)} rows)"
        else:
            result['message'] = "❌ No data was successfully fetched for any ticker"
        
        return result