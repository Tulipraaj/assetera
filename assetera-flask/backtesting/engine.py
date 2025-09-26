"""
Backtesting engine converted from Streamlit app
Now uses Snowflake data instead of Yahoo Finance API
"""

import math
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional
import numpy as np
import pandas as pd
from .funds import FUNDS, BENCHMARKS
from data.snowflake_client import get_snowflake_client
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)
TRADING_DAYS = 252

class BacktestingEngine:
    def __init__(self):
        self.snowflake_client = get_snowflake_client()
        self.cache = {}  # Simple in-memory cache
        self.cache_ttl = 3600 * 4  # 4 hours (longer since data is from database)
    
    def _get_cache_key(self, tickers: List[str], start_date: date, end_date: date) -> str:
        ticker_str = "_".join(sorted(tickers))
        return f"{ticker_str}_{start_date}_{end_date}"
    
    def _is_cache_valid(self, cache_entry: dict) -> bool:
        return time.time() - cache_entry['timestamp'] < self.cache_ttl

    def fetch_prices(self, tickers: List[str], start: date, end: date) -> pd.DataFrame:
        """
        Fetch prices from Snowflake database instead of Yahoo Finance
        """
        tickers = list(tickers)
        cache_key = self._get_cache_key(tickers, start, end)
        
        # Check cache first
        if cache_key in self.cache and self._is_cache_valid(self.cache[cache_key]):
            print(f"✓ Using cached data for {len(tickers)} tickers")
            return self.cache[cache_key]['data']
        
        try:
            # Fetch from Snowflake
            df = self.snowflake_client.fetch_price_data(tickers, start, end)
            
            if not df.empty:
                # Cache the result
                self.cache[cache_key] = {
                    'data': df,
                    'timestamp': time.time()
                }
                print(f"✅ Successfully fetched data for {len(df.columns)} tickers ({len(df)} rows) from Snowflake")
                return df
            else:
                print("❌ No data was returned from Snowflake")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error fetching data from Snowflake: {str(e)}")
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
        print("before multiplicaion")
        equity[0] = 1.0 * (1 - fee_daily) * (1.0 + (w_curr * R[0]).sum())
        print("after multiplication")
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
        logger.info("Tjios is before cagr")
        cagr = (float(eq.iloc[-1] / eq.iloc[0])) ** (1 / yrs) - 1 if yrs > 0 else float("nan")
        logger.info("This is befpre vol")
        vol = float(daily.std() * math.sqrt(TRADING_DAYS)) if len(daily) > 1 else float("nan")
        rf_daily = rf_annual / TRADING_DAYS
        logger.info("This is before sharpe")
        sharpe = float(((daily.mean() - rf_daily) / daily.std()) * math.sqrt(TRADING_DAYS)) if daily.std() > 0 else float("nan")
        logger.info("This is from KPIS")
        final_value = start_amount * float(eq.iloc[-1])
        logger.info("everuthing fine in KPI")
        
        return {
            "final_value": final_value, 
            "abs_return": total, 
            "cagr": cagr, 
            "vol": vol, 
            "sharpe": sharpe
        }

    def yearly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate yearly returns"""
        y = eq.resample("YE").last()
        yr = y.pct_change().dropna()
        yr.index = yr.index.year
        return yr

    def monthly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate monthly returns"""
        m = eq.resample("ME").last()
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
        Run complete backtest for a fund using Snowflake data
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
        
        # Fetch all needed data from Snowflake
        needed = sorted(fund_tickers | bench_tickers)
        prices = self.fetch_prices(needed, start_date, end_date)
        prices = prices.astype(float)
        # print(prices.info(),"thjis is ingo")
        # print(prices.head())
        if prices.empty:
            raise ValueError("No price data returned from Snowflake. Check your date range and ticker availability.")
        
        # Check which fund tickers are available
        present = [t for t in fund_tickers if t in prices.columns]
        missing = sorted(list(fund_tickers - set(present)))
        
        if not present:
            raise ValueError("None of the fund's tickers have data for the selected range.")
        
        # Renormalize weights for available tickers
        weights = self._renormalize(target_weights, present)
        print("After renormalize")
        
        # Calculate returns
        rets_all = self.compute_daily_returns(prices)
        r_port = rets_all[present].dropna(how="any")
        
        if r_port.empty or len(r_port) < 2:
            raise ValueError("Not enough overlapping data for the selected dates. Try a later start date.")
        print("after daily returns")
        # Calculate portfolio equity curve
        print("before eq")
        eq = self.iterate_portfolio_path(r_port, weights, rebalance=rebalance, fee_annual=fee_annual)
        print("after eq")
        if eq.empty:
            raise ValueError("Portfolio series is empty after alignment. Try different dates.")
        print("after iterate_portfolio_path")
        # Normalize to start at 1
        eq = eq / eq.iloc[0]
        effective_start, effective_end = eq.index[0].date(), eq.index[-1].date()
        
        # Calculate KPIs
        kpi = self.kpis_from_equity(eq, start_amount, effective_start, effective_end)
        
        # Calculate additional metrics
        mret = self.monthly_returns(eq)
        pct_pos_months = (mret > 0).mean() if not mret.empty else np.nan
        print("after mert")
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
                            'dates': [d.isoformat() for d in s.index],
                            'data': s.tolist()
                        })
        print("after bench_series")
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
        print("after yearly returns")
        # Calculate rolling 12-month returns
        roll12 = self.rolling_12m(eq)
        rolling_data = {
            'dates': [d.isoformat() for d in roll12.index] if not roll12.empty else [],
            'returns': roll12.tolist() if not roll12.empty else []
        }
        print("after rolling12m")
        # Calculate distribution stats
        var95 = float(np.percentile(mret, 5)) if not mret.empty else np.nan
        cvar95 = float(mret[mret <= var95].mean()) if not mret.empty and (mret <= var95).any() else var95
        print("after dist stats")
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