"""
Backtesting engine converted from Streamlit app
Handles data fetching, portfolio calculations, and performance metrics
"""

import math
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
import requests
import json
from .funds import FUNDS, BENCHMARKS

TRADING_DAYS = 252

class BacktestingEngine:
    def __init__(self):
        self.cache = {}  # Simple in-memory cache
        self.cache_ttl = 3600 * 2  # 2 hours
    
    def _get_cache_key(self, symbol: str, start_date: date, end_date: date) -> str:
        return f"{symbol}_{start_date}_{end_date}"
    
    def _is_cache_valid(self, cache_entry: dict) -> bool:
        return time.time() - cache_entry['timestamp'] < self.cache_ttl
    
    def manual_yahoo_fetch(self, symbol: str, start_date: date, end_date: date) -> pd.Series:
        """Fetch data for a single symbol using Yahoo Finance API directly"""
        
        cache_key = self._get_cache_key(symbol, start_date, end_date)
        if cache_key in self.cache and self._is_cache_valid(self.cache[cache_key]):
            return self.cache[cache_key]['data']
        
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
                                
                                # Cache the result
                                self.cache[cache_key] = {
                                    'data': series,
                                    'timestamp': time.time()
                                }
                                
                                return series
            
            print(f"Failed to fetch {symbol}: Status {response.status_code}")
            
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")
        
        return pd.Series(dtype=float, name=symbol)

    def fetch_prices(self, tickers: List[str], start: date, end: date) -> pd.DataFrame:
        """Fetch prices using manual Yahoo Finance API calls"""
        
        tickers = list(tickers)
        all_data = {}
        failed_tickers = []
        
        for ticker in tickers:
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
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.index.name = "Date"
            df = df.sort_index()
            
            # Forward fill missing values
            df = df.ffill()
            
            print(f"✅ Successfully fetched data for {len(df.columns)} tickers ({len(df)} rows)")
            return df
        else:
            print("❌ No data was successfully fetched for any ticker")
            return pd.DataFrame()

    def compute_daily_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        """Compute daily returns from price data"""
        return prices.pct_change()

    def _renormalize(self, weights: Dict[str, float], keep: List[str]) -> Dict[str, float]:
        """Renormalize weights for available tickers"""
        filt = {t: w for t, w in weights.items() if t in keep and w > 0}
        s = sum(filt.values())
        return {t: w / s for t, w in filt.items()} if s > 0 else {}

    def iterate_portfolio_path(self, returns: pd.DataFrame, weights: Dict[str, float],
                               rebalance: str = "Annual", fee_annual: float = 0.0) -> pd.Series:
        """Calculate portfolio equity curve with rebalancing and fees"""
        if not weights:
            return pd.Series(dtype=float)
        
        tickers = list(weights.keys())
        w_target = np.array([weights[t] for t in tickers], dtype=float)
        r = returns[tickers].dropna(how="any")
        
        if r.empty:
            return pd.Series(dtype=float)
        
        fee_daily = fee_annual / TRADING_DAYS if fee_annual > 0 else 0.0
        idx, R = r.index, r.values
        w_curr = w_target.copy()
        equity = np.zeros(len(idx), dtype=float)
        equity[0] = 1.0 * (1 - fee_daily) * (1.0 + (w_curr * R[0]).sum())
        
        for i in range(1, len(idx)):
            w_curr = w_curr * (1.0 + R[i - 1])
            s = w_curr.sum()
            w_curr = w_curr / s if s != 0 else w_target.copy()
            
            if rebalance.lower() == "annual" and idx[i - 1].year != idx[i].year:
                w_curr = w_target.copy()
            
            day_ret = (w_curr * R[i]).sum()
            equity[i] = equity[i - 1] * (1 - fee_daily) * (1.0 + day_ret)
        
        return pd.Series(equity, index=idx, name="Portfolio")

    def mix_benchmark(self, returns: pd.DataFrame, definition: dict) -> pd.Series:
        """Create benchmark series from definition"""
        t = definition.get("type")
        
        if t == "single":
            ticker = definition["ticker"]
            if ticker not in returns.columns:
                return pd.Series(dtype=float)
            r = returns[[ticker]].dropna(how="any")
            if r.empty: 
                return pd.Series(dtype=float)
            return (1 + r[ticker]).cumprod().rename(ticker)
        
        elif t == "mix":
            weights = definition["weights"]
            cols = [c for c in returns.columns if c in weights]
            if not cols: 
                return pd.Series(dtype=float)
            r = returns[cols].dropna(how="any")
            if r.empty: 
                return pd.Series(dtype=float)
            w = np.array([weights[c] for c in cols], dtype=float)
            w = w / w.sum()
            port = (1 + r).dot(w)
            return port.cumprod().rename("mix")
        
        return pd.Series(dtype=float)

    def years_between(self, d0: date, d1: date) -> float:
        """Calculate years between two dates"""
        return max((d1 - d0).days, 0) / 365.25

    def kpis_from_equity(self, eq: pd.Series, start_amount: float, start_date: date, 
                        end_date: date, rf_annual: float = 0.0) -> dict:
        """Calculate key performance indicators from equity curve"""
        if eq.empty or len(eq) < 2:
            return None
        
        daily = eq.pct_change().dropna()
        total = float(eq.iloc[-1] / eq.iloc[0] - 1.0)
        yrs = self.years_between(start_date, end_date)
        cagr = (float(eq.iloc[-1] / eq.iloc[0])) ** (1 / yrs) - 1 if yrs > 0 else float("nan")
        vol = float(daily.std() * math.sqrt(TRADING_DAYS)) if len(daily) > 1 else float("nan")
        rf_daily = rf_annual / TRADING_DAYS
        sharpe = float(((daily.mean() - rf_daily) / daily.std()) * math.sqrt(TRADING_DAYS)) if daily.std() > 0 else float("nan")
        final_value = start_amount * float(eq.iloc[-1])
        
        return {
            "final_value": final_value, 
            "abs_return": total, 
            "cagr": cagr, 
            "vol": vol, 
            "sharpe": sharpe
        }

    def yearly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate yearly returns"""
        y = eq.resample("Y").last()
        yr = y.pct_change().dropna()
        yr.index = yr.index.year
        return yr

    def monthly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate monthly returns"""
        m = eq.resample("M").last()
        mr = m.pct_change().dropna()
        mr.index = mr.index.to_period("M")
        return mr

    def rolling_12m(self, eq: pd.Series) -> pd.Series:
        """Calculate rolling 12-month returns"""
        r = eq / eq.shift(TRADING_DAYS) - 1.0
        return r.dropna()

    def run_backtest(self, fund_id: str, start_date: date, end_date: date, 
                    start_amount: float = 100000, benchmarks: List[str] = None,
                    rebalance: str = "Annual", fee_annual: float = None) -> dict:
        """
        Run complete backtest for a fund
        Returns dictionary with all results for web display
        """
        if fund_id not in FUNDS:
            raise ValueError(f"Unknown fund: {fund_id}")
        
        if benchmarks is None:
            benchmarks = ["SPY", "60/40", "GLD"]
        
        fund = FUNDS[fund_id]
        target_weights = fund["allocations"].copy()
        fund_tickers = set(target_weights.keys())
        
        if fee_annual is None:
            fee_annual = fund["default_fee"]
        
        # Get benchmark tickers
        bench_tickers = set()
        for b in benchmarks:
            if b in BENCHMARKS:
                d = BENCHMARKS[b]["definition"]
                if d["type"] == "single": 
                    bench_tickers.add(d["ticker"])
                else: 
                    bench_tickers.update(d["weights"].keys())
        
        # Fetch all needed data
        needed = sorted(fund_tickers | bench_tickers)
        prices = self.fetch_prices(needed, start_date, end_date)
        
        if prices.empty:
            raise ValueError("No price data returned. Try a later start date or check your network.")
        
        # Check which fund tickers are available
        present = [t for t in fund_tickers if t in prices.columns]
        missing = sorted(list(fund_tickers - set(present)))
        
        if not present:
            raise ValueError("None of the fund's tickers have data for the selected range.")
        
        # Renormalize weights for available tickers
        weights = self._renormalize(target_weights, present)
        
        # Calculate returns
        rets_all = self.compute_daily_returns(prices)
        r_port = rets_all[present].dropna(how="any")
        
        if r_port.empty or len(r_port) < 2:
            raise ValueError("Not enough overlapping data for the selected dates. Try a later start date.")
        
        # Calculate portfolio equity curve
        eq = self.iterate_portfolio_path(r_port, weights, rebalance=rebalance, fee_annual=fee_annual)
        
        if eq.empty:
            raise ValueError("Portfolio series is empty after alignment. Try different dates.")
        
        # Normalize to start at 1
        eq = eq / eq.iloc[0]
        effective_start, effective_end = eq.index[0].date(), eq.index[-1].date()
        
        # Calculate KPIs
        kpi = self.kpis_from_equity(eq, start_amount, effective_start, effective_end)
        
        # Calculate additional metrics
        mret = self.monthly_returns(eq)
        pct_pos_months = (mret > 0).mean() if not mret.empty else np.nan
        
        # Calculate benchmark series
        bench_series = []
        for b in benchmarks:
            if b in BENCHMARKS:
                s = self.mix_benchmark(rets_all, BENCHMARKS[b]["definition"])
                if not s.empty:
                    s = s.reindex(eq.index).ffill().dropna()
                    if not s.empty:
                        s = s / s.iloc[0]
                        bench_series.append({
                            'name': BENCHMARKS[b]["name"],
                            'data': s.to_dict()
                        })
        
        # Prepare chart data
        chart_data = {
            'dates': [d.isoformat() for d in eq.index],
            'portfolio': eq.tolist(),
            'benchmarks': bench_series
        }
        
        # Calculate yearly returns
        yr = self.yearly_returns(eq)
        yearly_data = {
            'years': yr.index.tolist() if not yr.empty else [],
            'returns': yr.tolist() if not yr.empty else []
        }
        
        # Calculate rolling 12-month returns
        roll12 = self.rolling_12m(eq)
        rolling_data = {
            'dates': [d.isoformat() for d in roll12.index] if not roll12.empty else [],
            'returns': roll12.tolist() if not roll12.empty else []
        }
        
        # Calculate distribution stats
        var95 = float(np.percentile(mret, 5)) if not mret.empty else np.nan
        cvar95 = float(mret[mret <= var95].mean()) if not mret.empty and (mret <= var95).any() else var95
        
        return {
            'fund_id': fund_id,
            'fund_name': fund['name'],
            'effective_start': effective_start.isoformat(),
            'effective_end': effective_end.isoformat(),
            'missing_tickers': missing,
            'kpis': kpi,
            'pct_pos_months': pct_pos_months,
            'chart_data': chart_data,
            'yearly_data': yearly_data,
            'rolling_data': rolling_data,
            'distribution': {
                'var95': var95,
                'cvar95': cvar95,
                'avg_monthly': float(mret.mean()) if not mret.empty else np.nan,
                'monthly_returns': mret.tolist() if not mret.empty else []
            },
            'settings': {
                'start_amount': start_amount,
                'rebalance': rebalance,
                'fee_annual': fee_annual
            }
        }
