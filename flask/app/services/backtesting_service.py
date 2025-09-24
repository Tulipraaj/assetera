import math
import numpy as np
import pandas as pd
from datetime import date
from typing import Dict, List, Tuple, Optional
from app.config import Config

class BacktestingService:
    
    def __init__(self):
        self.TRADING_DAYS = Config.TRADING_DAYS
    
    def years_between(self, d0: date, d1: date) -> float:
        return max((d1 - d0).days, 0) / 365.25
    
    def compute_daily_returns(self, prices: pd.DataFrame) -> pd.DataFrame:
        return prices.pct_change()
    
    def renormalize_weights(self, weights: Dict[str, float], keep: List[str]) -> Dict[str, float]:
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
        
        fee_daily = fee_annual / self.TRADING_DAYS if fee_annual > 0 else 0.0
        idx, R = r.index, r.values
        w_curr = w_target.copy()
        equity = np.zeros(len(idx), dtype=float)
        
        # Initialize first day
        equity[0] = 1.0 * (1 - fee_daily) * (1.0 + (w_curr * R[0]).sum())
        
        for i in range(1, len(idx)):
            # Update weights based on previous day returns
            w_curr = w_curr * (1.0 + R[i - 1])
            s = w_curr.sum()
            w_curr = w_curr / s if s != 0 else w_target.copy()
            
            # Annual rebalancing
            if rebalance.lower() == "annual" and idx[i - 1].year != idx[i].year:
                w_curr = w_target.copy()
            
            # Calculate day return and apply fees
            day_ret = (w_curr * R[i]).sum()
            equity[i] = equity[i - 1] * (1 - fee_daily) * (1.0 + day_ret)
        
        return pd.Series(equity, index=idx, name="Portfolio")
    
    def calculate_kpis(self, eq: pd.Series, start_amount: float, start_date: date, 
                      end_date: date, rf_annual: float = 0.0) -> Dict:
        """Calculate portfolio KPIs"""
        
        if eq.empty or len(eq) < 2:
            return {}
        
        daily = eq.pct_change().dropna()
        total = float(eq.iloc[-1] / eq.iloc[0] - 1.0)
        yrs = self.years_between(start_date, end_date)
        cagr = (float(eq.iloc[-1] / eq.iloc[0])) ** (1 / yrs) - 1 if yrs > 0 else float("nan")
        vol = float(daily.std() * math.sqrt(self.TRADING_DAYS)) if len(daily) > 1 else float("nan")
        
        rf_daily = rf_annual / self.TRADING_DAYS
        sharpe = float(((daily.mean() - rf_daily) / daily.std()) * math.sqrt(self.TRADING_DAYS)) if daily.std() > 0 else float("nan")
        
        final_value = start_amount * float(eq.iloc[-1])
        
        # Monthly stats
        monthly_ret = self.monthly_returns(eq)
        pct_pos_months = (monthly_ret > 0).mean() if not monthly_ret.empty else np.nan
        
        return {
            "final_value": final_value,
            "abs_return": total,
            "cagr": cagr,
            "vol": vol,
            "sharpe": sharpe,
            "pct_pos_months": pct_pos_months
        }
    
    def monthly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate monthly returns"""
        m = eq.resample("M").last()
        mr = m.pct_change().dropna()
        mr.index = mr.index.to_period("M")
        return mr
    
    def yearly_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate yearly returns"""
        y = eq.resample("Y").last()
        yr = y.pct_change().dropna()
        yr.index = yr.index.year
        return yr
    
    def rolling_12m_returns(self, eq: pd.Series) -> pd.Series:
        """Calculate rolling 12-month returns"""
        r = eq / eq.shift(self.TRADING_DAYS) - 1.0
        return r.dropna()
    
    def mix_benchmark(self, returns: pd.DataFrame, definition: dict) -> pd.Series:
        """Calculate benchmark performance"""
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
    
    def format_money(self, x, currency="USD"):
        """Format money values"""
        sym = "$" if currency.upper() == "USD" else "₹"
        try: 
            return f"{sym}{x:,.0f}"
        except Exception: 
            return f"{sym}{x}"
    
    def format_percent(self, x):
        """Format percentage values"""
        try: 
            return f"{x * 100:.2f}%"
        except Exception: 
            return "—"