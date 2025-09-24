# streamlit_app.py — Public Streamlit app using API backend
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import requests
from datetime import date
import math
from typing import Dict, List
import json

TRADING_DAYS = 252

# Configuration
API_BASE_URL = "https://your-api-url.com"  # Replace with your deployed API URL

st.set_page_config(
    page_title="AssetEra — Fund Backtester (Prototype)",
    page_icon="📈",
    layout="wide",
)

# CSS styling (same as before)
st.markdown("""
<style>
  :root {
    --accent: #2653F0;
    --accent-hover: #1E3ED8;
    --bg: #F7FAFC;
    --panel: #FFFFFF;
    --text: #0F172A;
    --muted: #475569;
    --border: #E5E7EB;
    --shadow: 0 6px 20px rgba(17,24,39,0.06);
    --radius: 14px;
  }
  .stApp { background: var(--bg); color: var(--text); }
  .ae-hero {
    background: linear-gradient(180deg, #ffffff 0%, #f6f8ff 100%);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 16px 18px;
    box-shadow: var(--shadow);
    margin-bottom: 12px;
  }
  .stButton>button, .stDownloadButton>button {
    background: var(--accent) !important;
    color: #fff !important;
    border: 1px solid var(--accent);
    border-radius: 10px;
    font-weight: 600;
    padding: 8px 14px;
    box-shadow: 0 6px 16px rgba(38,83,240,.15);
  }
  .stMetric {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 12px 14px;
    box-shadow: var(--shadow);
  }
  h1,h2,h3 { color: var(--text); }
  .ae-muted { color: var(--muted); font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# API calling functions
@st.cache_data(ttl=3600)
def fetch_funds():
    """Fetch fund data from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/funds", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch funds: {response.status_code}")
            return {}
    except Exception as e:
        st.error(f"Error fetching funds: {str(e)}")
        return {}

@st.cache_data(ttl=3600)
def fetch_benchmarks():
    """Fetch benchmark data from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/benchmarks", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch benchmarks: {response.status_code}")
            return {}
    except Exception as e:
        st.error(f"Error fetching benchmarks: {str(e)}")
        return {}

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def fetch_prices(tickers: List[str], start_date: date, end_date: date):
    """Fetch price data from API"""
    try:
        ticker_str = ",".join(tickers)
        params = {
            "tickers": ticker_str,
            "start_date": str(start_date),
            "end_date": str(end_date)
        }
        
        response = requests.get(f"{API_BASE_URL}/prices", params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()["data"]
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Pivot to get tickers as columns
            price_df = df.pivot(index='date', columns='ticker', values='price')
            price_df.index.name = "Date"
            
            # Sort and forward fill
            price_df = price_df.sort_index().ffill()
            
            return price_df
        else:
            st.error(f"Failed to fetch prices: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching prices: {str(e)}")
        return pd.DataFrame()

# Load fund and benchmark data
FUNDS = fetch_funds()
BENCHMARKS = fetch_benchmarks()

# Utility functions (same as your original app)
def clamp_end_date(d: date) -> date:
    return min(d, date.today())

def years_between(d0: date, d1: date) -> float:
    return max((d1 - d0).days, 0) / 365.25

def compute_daily_returns(prices: pd.DataFrame) -> pd.DataFrame:
    return prices.pct_change()

def _renormalize(weights: Dict[str, float], keep: List[str]) -> Dict[str, float]:
    filt = {t: w for t, w in weights.items() if t in keep and w > 0}
    s = sum(filt.values())
    return {t: w / s for t, w in filt.items()} if s > 0 else {}

def iterate_portfolio_path(returns: pd.DataFrame, weights: Dict[str, float],
                           rebalance: str = "Annual", fee_annual: float = 0.0) -> pd.Series:
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

def mix_benchmark(returns: pd.DataFrame, definition: dict) -> pd.Series:
    t = definition.get("type")
    if t == "single":
        ticker = definition["ticker"]
        if ticker not in returns.columns:
            return pd.Series(dtype=float)
        r = returns[[ticker]].dropna(how="any")
        if r.empty: return pd.Series(dtype=float)
        return (1 + r[ticker]).cumprod().rename(ticker)
    elif t == "mix":
        weights = definition["weights"]
        cols = [c for c in returns.columns if c in weights]
        if not cols: return pd.Series(dtype=float)
        r = returns[cols].dropna(how="any")
        if r.empty: return pd.Series(dtype=float)
        w = np.array([weights[c] for c in cols], dtype=float); w = w / w.sum()
        port = (1 + r).dot(w)
        return port.cumprod().rename("mix")
    return pd.Series(dtype=float)

def kpis_from_equity(eq: pd.Series, start_amount: float, start_date: date, end_date: date,
                     rf_annual: float = 0.0):
    if eq.empty or len(eq) < 2:
        return None, None
    daily = eq.pct_change().dropna()
    total = float(eq.iloc[-1] / eq.iloc[0] - 1.0)
    yrs = years_between(start_date, end_date)
    cagr = (float(eq.iloc[-1] / eq.iloc[0])) ** (1 / yrs) - 1 if yrs > 0 else float("nan")
    vol = float(daily.std() * math.sqrt(TRADING_DAYS)) if len(daily) > 1 else float("nan")
    rf_daily = rf_annual / TRADING_DAYS
    sharpe = float(((daily.mean() - rf_daily) / daily.std()) * math.sqrt(TRADING_DAYS)) if daily.std() > 0 else float("nan")
    final_value = start_amount * float(eq.iloc[-1])
    return {"final_value": final_value, "abs_return": total, "cagr": cagr, "vol": vol, "sharpe": sharpe}, None

def yearly_returns(eq: pd.Series) -> pd.Series:
    y = eq.resample("Y").last()
    yr = y.pct_change().dropna()
    yr.index = yr.index.year
    return yr

def monthly_returns(eq: pd.Series) -> pd.Series:
    m = eq.resample("M").last()
    mr = m.pct_change().dropna()
    mr.index = mr.index.to_period("M")
    return mr

def rolling_12m(eq: pd.Series) -> pd.Series:
    r = eq / eq.shift(TRADING_DAYS) - 1.0
    return r.dropna()

def money(x, cur="USD"):
    sym = "$" if cur.upper() == "USD" else "₹"
    try: return f"{sym}{x:,.0f}"
    except Exception: return f"{sym}{x}"

def perc(x):
    try: return f"{x * 100:.2f}%"
    except Exception: return "—"

# Header
st.markdown("""
<div class="ae-hero">
  <div style="display:flex;align-items:center;gap:12px;">
    <div style="font-weight:800;font-size:28px;color:#0F172A;">AssetEra</div>
    <div class="ae-muted">| Public Backtesting Demo</div>
  </div>
  <div class="ae-muted" style="margin-top:4px;">
    Portfolio backtesting tool - Data sourced from secure API. For demonstration purposes only.
  </div>
</div>
""", unsafe_allow_html=True)

# Check if data loaded successfully
if not FUNDS or not BENCHMARKS:
    st.error("Unable to load fund and benchmark data. Please check API connection.")
    st.stop()

# Sidebar controls (same as before)
st.sidebar.header("Controls")

fund_id = st.sidebar.selectbox(
    "Fund",
    options=list(FUNDS.keys()),
    format_func=lambda k: f"{k} — {FUNDS[k]['name']}",
)

today = date.today()
default_start = date(today.year - 5, today.month, today.day)  # Default to 5 years for public demo
start_date = st.sidebar.date_input("Start date", value=default_start, max_value=today)
end_date = st.sidebar.date_input("End date", value=today, max_value=today)

if isinstance(start_date, tuple):
    start_date = start_date[0]
if isinstance(end_date, tuple):
    end_date = end_date[0]

end_date = clamp_end_date(end_date)
if start_date >= end_date:
    st.sidebar.warning("Start date must be BEFORE end date.")

start_amount = st.sidebar.number_input("Starting amount", min_value=1000.0, value=100000.0, step=1000.0)
currency = st.sidebar.selectbox("Currency (label only)", options=["USD", "INR"], index=0)

rebalance = st.sidebar.selectbox("Rebalance", options=["Annual", "None"], index=0)
fee_toggle = st.sidebar.checkbox("Apply annual fee (TER)", value=True)
fee_value = st.sidebar.number_input(
    "Annual fee (e.g., 0.20% → 0.0020)",
    min_value=0.0, max_value=0.05,
    value=FUNDS[fund_id]["default_fee"] if fund_id in FUNDS else 0.002,
    step=0.0005, format="%.4f",
)

default_bench = ["SPY", "60/40", "GLD"]
available_benchmarks = [b for b in default_bench if b in BENCHMARKS]
bench_sel = st.sidebar.multiselect("Benchmarks (max 3)", options=list(BENCHMARKS.keys()), default=available_benchmarks)
if len(bench_sel) > 3:
    bench_sel = bench_sel[:3]

if st.sidebar.button("↻ Clear cache"):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared.")

run = st.sidebar.button("▶️ Run backtest", type="primary")

# Main computation
if run:
    with st.spinner("Fetching data and computing..."):
        target_weights = FUNDS[fund_id]["allocations"].copy()
        fund_tickers = set(target_weights.keys())

        bench_tickers = set()
        for b in bench_sel:
            d = BENCHMARKS[b]["definition"]
            if d["type"] == "single": bench_tickers.add(d["ticker"])
            else: bench_tickers.update(d["weights"].keys())

        needed = sorted(fund_tickers | bench_tickers)
        prices = fetch_prices(needed, start_date, end_date)

        if prices.empty:
            st.error("No price data returned. Please try a different date range.")
            st.stop()

        present = [t for t in fund_tickers if t in prices.columns]
        missing = sorted(list(fund_tickers - set(present)))
        if missing:
            st.warning(f"These tickers had no data and were dropped: {missing}")

        weights = _renormalize(target_weights, present)
        if not weights:
            st.error("No valid tickers found for the selected range.")
            st.stop()

        rets_all = compute_daily_returns(prices)
        r_port = rets_all[present].dropna(how="any")
        
        if r_port.empty or len(r_port) < 2:
            st.error("Not enough data for the selected dates.")
            st.stop()

        fee = float(fee_value) if fee_toggle else 0.0
        eq = iterate_portfolio_path(r_port, weights, rebalance=rebalance, fee_annual=fee)
        
        if eq.empty:
            st.error("Portfolio calculation failed.")
            st.stop()

        eq = eq / eq.iloc[0]
        effective_start, effective_end = eq.index[0].date(), eq.index[-1].date()

        kpi, _ = kpis_from_equity(eq, float(start_amount), effective_start, effective_end)
        mret = monthly_returns(eq)
        pct_pos_months = (mret > 0).mean() if not mret.empty else np.nan

        bench_series = []
        for b in bench_sel:
            s = mix_benchmark(rets_all, BENCHMARKS[b]["definition"])
            if not s.empty:
                s = s.reindex(eq.index).ffill().dropna()
                if not s.empty:
                    s = s / s.iloc[0]
                    s.name = BENCHMARKS[b]["name"]
                    bench_series.append(s)

        # Display results (same as original)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Final Value", money(kpi["final_value"], currency))
        c2.metric("Absolute Return", perc(kpi["abs_return"]))
        c3.metric("CAGR", perc(kpi["cagr"]) if np.isfinite(kpi["cagr"]) else "—")
        c4.metric("Volatility (ann.)", perc(kpi["vol"]) if np.isfinite(kpi["vol"]) else "—")
        c5.metric("% Positive Months", perc(pct_pos_months) if np.isfinite(pct_pos_months) else "—")

        # Chart
        df_plot = pd.DataFrame({"Portfolio": eq})
        for s in bench_series:
            df_plot[s.name] = s.reindex(df_plot.index, method="ffill")
        
        fig_eq = px.line(
            df_plot, x=df_plot.index, y=df_plot.columns,
            template="plotly_white",
            labels={"value": "Growth (× starting value)", "index": "Date"},
            title="Portfolio Performance",
        )
        st.plotly_chart(fig_eq, use_container_width=True)

        st.markdown("---")
        st.write(f"**Fund:** {FUNDS[fund_id]['name']}")
        st.write(f"**Period:** {effective_start} to {effective_end}")
        st.write("**Disclaimer:** For demonstration purposes only. Not investment advice.")

else:
    st.info("Configure your parameters and click 'Run backtest' to analyze portfolio performance.")