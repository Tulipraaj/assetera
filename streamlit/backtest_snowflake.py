# backtester_app.py — AssetEra Fund Backtester (Snowflake version)
# Modified to load data from Snowflake instead of external APIs

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
import warnings

TRADING_DAYS = 252
warnings.filterwarnings('ignore')

# =========================
# Snowflake Configuration
# =========================
@st.cache_resource
def init_snowflake_connection():
    """Initialize Snowflake connection using Streamlit secrets"""
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"]
    )

# =========================
# Branding & page settings
# =========================
st.set_page_config(
    page_title="AssetEra — Fund Backtester (Prototype)",
    page_icon="📈",
    layout="wide",
)

# Light theme CSS (same as before)
st.markdown(
    """
    <style>
      :root {
        --accent: #2653F0;
        --accent-hover: #1E3ED8;
        --bg: #F7FAFC;         /* page */
        --panel: #FFFFFF;      /* cards/panels */
        --text: #0F172A;       /* slate-900 */
        --muted: #475569;      /* slate-600 */
        --border: #E5E7EB;     /* gray-200 */
        --shadow: 0 6px 20px rgba(17,24,39,0.06);
        --radius: 14px;
      }
      .stApp { background: var(--bg); color: var(--text); }
      /* Header band */
      .ae-hero {
        background: linear-gradient(180deg, #ffffff 0%, #f6f8ff 100%);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 16px 18px;
        box-shadow: var(--shadow);
        margin-bottom: 12px;
      }
      /* Sidebar */
      section[data-testid="stSidebar"] {
        background: #FFFFFF !important;
        border-right: 1px solid var(--border);
      }
      /* Buttons */
      .stButton>button, .stDownloadButton>button {
        background: var(--accent) !important;
        color: #fff !important;
        border: 1px solid var(--accent);
        border-radius: 10px;
        font-weight: 600;
        padding: 8px 14px;
        transition: all .15s ease;
        box-shadow: 0 6px 16px rgba(38,83,240,.15);
      }
      .stButton>button:hover, .stDownloadButton>button:hover {
        background: var(--accent-hover) !important;
        border-color: var(--accent-hover);
        transform: translateY(-1px);
        box-shadow: 0 10px 18px rgba(30,62,216,.18);
      }
      /* KPI tiles */
      .stMetric {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 12px 14px;
        box-shadow: var(--shadow);
      }
      /* Headings */
      h1,h2,h3 { color: var(--text); }
      /* Small captions / muted text */
      .ae-muted { color: var(--muted); font-size: 13px; }
      /* Plotly font */
      .js-plotly-plot .plotly .main-svg {
        font-family: "Inter", system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# =========================
# Fund definitions (same as before)
# =========================
@st.cache_data
def load_funds_from_snowflake():
    """Load fund definitions from Snowflake"""
    try:
        conn = init_snowflake_connection()
        cursor = conn.cursor()
        
        # Get fund data
        cursor.execute("SELECT FUND_ID, FUND_NAME, TICKER, WEIGHT FROM FUND_ALLOCATIONS ORDER BY FUND_ID, TICKER")
        fund_data = cursor.fetchall()
        
        funds = {}
        for fund_id, fund_name, ticker, weight in fund_data:
            if fund_id not in funds:
                funds[fund_id] = {
                    "name": fund_name,
                    "allocations": {},
                    "default_fee": 0.002,
                    "default_rebalance": "Annual"
                }
            funds[fund_id]["allocations"][ticker] = float(weight)
        
        cursor.close()
        return funds
        
    except Exception as e:
        st.error(f"Error loading funds from Snowflake: {str(e)}")
        # Fallback to hardcoded funds
        return {
            "F1": {
                "name": "Fund 1 — Core Income (low risk)",
                "allocations": {
                    "LQD": 0.50, "IEF": 0.20, "GLD": 0.10, "VEA": 0.10,
                    "MSFT": 0.02, "APH": 0.02, "GWW": 0.02, "PH": 0.02, "BSX": 0.02,
                },
                "default_fee": 0.002,
                "default_rebalance": "Annual",
            }
        }

@st.cache_data
def load_benchmarks_from_snowflake():
    """Load benchmark definitions from Snowflake"""
    try:
        conn = init_snowflake_connection()
        cursor = conn.cursor()
        
        # Get benchmark data
        cursor.execute("SELECT BENCHMARK_ID, BENCHMARK_NAME, TICKER FROM BENCHMARKS")
        benchmark_data = cursor.fetchall()
        
        benchmarks = {}
        for bench_id, bench_name, ticker in benchmark_data:
            benchmarks[bench_id] = {
                "name": bench_name,
                "definition": {"type": "single", "ticker": ticker}
            }
        
        # Add hardcoded mixed benchmarks
        benchmarks.update({
            "60/40": {"name": "60/40 (SPY/IEF)", "definition": {"type": "mix", "weights": {"SPY": 0.6, "IEF": 0.4}}},
            "80/20": {"name": "80/20 (SPY/IEF)", "definition": {"type": "mix", "weights": {"SPY": 0.8, "IEF": 0.2}}},
            "40/60": {"name": "40/60 (SPY/IEF)", "definition": {"type": "mix", "weights": {"SPY": 0.4, "IEF": 0.6}}},
            "ALL_WEATHER_LITE": {
                "name": "All-weather lite (35 SPY / 35 IEF / 30 GLD)",
                "definition": {"type": "mix", "weights": {"SPY": 0.35, "IEF": 0.35, "GLD": 0.30}},
            },
            "EQ_GLD_70_30": {
                "name": "Equity + Gold (70 SPY / 30 GLD)",
                "definition": {"type": "mix", "weights": {"SPY": 0.70, "GLD": 0.30}},
            },
        })
        
        cursor.close()
        return benchmarks
        
    except Exception as e:
        st.error(f"Error loading benchmarks from Snowflake: {str(e)}")
        # Fallback to minimal benchmarks
        return {
            "SPY": {"name": "S&P 500 (SPY)", "definition": {"type": "single", "ticker": "SPY"}},
            "GLD": {"name": "Gold (GLD)", "definition": {"type": "single", "ticker": "GLD"}},
        }

# Load data from Snowflake
FUNDS = load_funds_from_snowflake()
BENCHMARKS = load_benchmarks_from_snowflake()

# ==================================================
# Utilities — fetching, returns, portfolio compounding
# ==================================================
def clamp_end_date(d: date) -> date:
    return min(d, date.today())

def years_between(d0: date, d1: date) -> float:
    return max((d1 - d0).days, 0) / 365.25

@st.cache_data(show_spinner=False, ttl="1H")
def fetch_prices_from_snowflake(tickers: List[str], start: date, end: date) -> pd.DataFrame:
    """
    Fetch prices from Snowflake database
    """
    if not tickers:
        return pd.DataFrame()
    
    try:
        conn = init_snowflake_connection()
        
        # Create the ticker list for SQL query
        ticker_list = "', '".join(tickers)
        
        # SQL query to get price data
        query = f"""
        SELECT DATE, TICKER, PRICE
        FROM DAILY_PRICES
        WHERE TICKER IN ('{ticker_list}')
        AND DATE >= '{start}'
        AND DATE <= '{end}'
        ORDER BY DATE, TICKER
        """
        
        # Execute query and get results
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        if not results:
            st.error("No data found in Snowflake for the selected date range and tickers.")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=['DATE', 'TICKER', 'PRICE'])
        
        # Pivot to get tickers as columns
        price_df = df.pivot(index='DATE', columns='TICKER', values='PRICE')
        
        # Ensure DATE index is datetime
        price_df.index = pd.to_datetime(price_df.index)
        price_df.index.name = "Date"
        
        # Sort by date and forward fill missing values
        price_df = price_df.sort_index().ffill()
        
        cursor.close()
        conn.close()
        
        st.success(f"✅ Successfully loaded data for {len(price_df.columns)} tickers ({len(price_df)} rows)")
        return price_df
        
    except Exception as e:
        st.error(f"Error fetching data from Snowflake: {str(e)}")
        return pd.DataFrame()

def compute_daily_returns(prices: pd.DataFrame) -> pd.DataFrame:
    # Keep NaNs; we will align/drop per use
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

# =========================
# Header / Branding (same as before)
# =========================
st.markdown(
    """
    <div class="ae-hero">
      <div style="display:flex;align-items:center;gap:12px;">
        <div style="font-weight:800;font-size:28px;color:#0F172A;">AssetEra</div>
        <div class="ae-muted">| Backtesting Workbench (Prototype)</div>
      </div>
      <div class="ae-muted" style="margin-top:4px;">
        An AssetEra research tool under Fidelity Investment — focused on five model funds. Data from Snowflake.
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# =========================
# SIDEBAR — Controls (same as before)
# =========================
st.sidebar.header("Controls")

fund_id = st.sidebar.selectbox(
    "Fund",
    options=list(FUNDS.keys()),
    format_func=lambda k: f"{k} — {FUNDS[k]['name']}",
)

today = date.today()
default_start = date(today.year - 10, today.month, today.day)
start_date = st.sidebar.date_input("Start date", value=default_start, max_value=today)
end_date = st.sidebar.date_input("End date (≤ today)", value=today, max_value=today)

if isinstance(start_date, tuple):  # legacy quirk
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
    value=FUNDS[fund_id]["default_fee"],
    step=0.0005, format="%.4f",
)
rf_rate = st.sidebar.number_input(
    "Risk-free annual rate (Sharpe)",
    min_value=0.0, max_value=0.2,
    value=0.0, step=0.0025, format="%.4f",
)

default_bench = ["SPY", "60/40", "GLD"]
bench_sel = st.sidebar.multiselect("Benchmarks (max 3)", options=list(BENCHMARKS.keys()), default=default_bench)
if len(bench_sel) > 3:
    bench_sel = bench_sel[:3]
    st.sidebar.info("Showing first 3 selected benchmarks for clarity.")

if st.sidebar.button("↻ Force refresh data cache"):
    st.cache_data.clear()
    st.sidebar.success("Cache cleared. Rerun the backtest.")

run = st.sidebar.button("▶️ Run backtest", type="primary")

# =========================
# MAIN — Compute & Render (modified to use Snowflake)
# =========================
if run:
    with st.spinner("Loading data from Snowflake & computing…"):
        target_weights = FUNDS[fund_id]["allocations"].copy()
        fund_tickers = set(target_weights.keys())

        bench_tickers = set()
        for b in bench_sel:
            d = BENCHMARKS[b]["definition"]
            if d["type"] == "single": bench_tickers.add(d["ticker"])
            else: bench_tickers.update(d["weights"].keys())

        needed = sorted(fund_tickers | bench_tickers)
        prices = fetch_prices_from_snowflake(needed, start_date, end_date)

        if prices.empty:
            st.error("No price data returned from Snowflake. Check your date range and ensure data is loaded.")
            st.stop()

        present = [t for t in fund_tickers if t in prices.columns]
        missing = sorted(list(fund_tickers - set(present)))
        if missing:
            st.warning(f"These tickers had no data in Snowflake for the date range and were dropped (weights renormalized): {missing}")

        weights = _renormalize(target_weights, present)
        if not weights:
            st.error("None of the fund's tickers have data for the selected range.")
            st.stop()

        rets_all = compute_daily_returns(prices)
        r_port = rets_all[present].dropna(how="any")
        if r_port.empty or len(r_port) < 2:
            st.error("Not enough overlapping data for the selected dates. Try a later start date.")
            st.stop()

        fee = float(fee_value) if fee_toggle else 0.0
        eq = iterate_portfolio_path(r_port, weights, rebalance=rebalance, fee_annual=fee)
        if eq.empty:
            st.error("Portfolio series is empty after alignment. Try different dates.")
            st.stop()

        eq = eq / eq.iloc[0]
        effective_start, effective_end = eq.index[0].date(), eq.index[-1].date()

        kpi, _ = kpis_from_equity(eq, float(start_amount), effective_start, effective_end, rf_annual=rf_rate)
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
            else:
                st.warning(f"No overlapping data for benchmark: {BENCHMARKS[b]['name']} — skipped.")

        # KPI strip (same as before)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Final Value", money(kpi["final_value"], currency))
        c2.metric("Absolute Return", perc(kpi["abs_return"]))
        c3.metric("CAGR", perc(kpi["cagr"]) if np.isfinite(kpi["cagr"]) else "—")
        c4.metric("Volatility (ann.)", perc(kpi["vol"]) if np.isfinite(kpi["vol"]) else "—")
        c5.metric("% Positive Months", perc(pct_pos_months) if np.isfinite(pct_pos_months) else "—")

        # Rest of the visualization code remains the same...
        # Equity curve (light template)
        df_plot = pd.DataFrame({"Portfolio": eq})
        for s in bench_series:
            df_plot[s.name] = s.reindex(df_plot.index, method="ffill")
        fig_eq = px.line(
            df_plot, x=df_plot.index, y=df_plot.columns,
            template="plotly_white",
            labels={"value": "Growth (× starting value)", "index": "Date"},
            title="Equity Curve (normalized to 1 at start)",
        )
        st.plotly_chart(fig_eq, use_container_width=True)

        # Analytics tabs (same as before)
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Rolling 12-month return", "Yearly returns", "Distribution & tail risk", "Upside/Downside capture"]
        )

        with tab1:
            roll12 = rolling_12m(eq)
            if not roll12.empty:
                fig_r = px.line(
                    roll12, template="plotly_white",
                    labels={"value": "Trailing 12-month return", "index": "Date"},
                    title="Rolling 12-month return (trailing)",
                )
                st.plotly_chart(fig_r, use_container_width=True)
                st.caption("Each point shows the return over the previous 12 months.")
            else:
                st.info("Not enough data for a 12-month rolling window.")

        with tab2:
            yr = yearly_returns(eq)
            if not yr.empty:
                fig_y = px.bar(
                    yr, template="plotly_white",
                    labels={"value": "Return", "index": "Year"},
                    title="Yearly returns — Portfolio",
                )
                st.plotly_chart(fig_y, use_container_width=True)
            else:
                st.info("Yearly view not available (short window).")

        with tab3:
            mr = monthly_returns(eq)
            if not mr.empty:
                var95 = float(np.percentile(mr, 5))
                cvar95 = float(mr[mr <= var95].mean()) if (mr <= var95).any() else var95
                fig_d = px.histogram(
                    mr, nbins=30, template="plotly_white",
                    labels={"value": "Monthly return", "index": "Months"},
                    title="Distribution of monthly returns",
                )
                st.plotly_chart(fig_d, use_container_width=True)
                k1, k2, k3 = st.columns(3)
                k1.metric("VaR 95% (monthly)", perc(var95))
                k2.metric("CVaR 95% (monthly)", perc(cvar95))
                k3.metric("Avg month", perc(float(mr.mean())))
                st.caption("VaR ~ typical worst 5% month. CVaR = average of the worst 5% months.")
            else:
                st.info("Need at least a few months of data for distribution stats.")

        with tab4:
            primary_key = bench_sel[0] if bench_sel else "SPY"
            b_eq = mix_benchmark(rets_all, BENCHMARKS[primary_key]["definition"])
            if not b_eq.empty:
                b_eq = b_eq.reindex(eq.index).ffill().dropna()
                if not b_eq.empty:
                    b_eq = b_eq / b_eq.iloc[0]
                    pm = monthly_returns(eq).to_timestamp()
                    bm = monthly_returns(b_eq).to_timestamp()
                    pair = pd.concat([pm.rename("fund"), bm.rename("bench")], axis=1).dropna()
                    up = pair[pair["bench"] > 0]
                    dn = pair[pair["bench"] < 0]
                    up_cap = float(up["fund"].mean() / up["bench"].mean()) if not up.empty else np.nan
                    dn_cap = float(dn["fund"].mean() / dn["bench"].mean()) if not dn.empty else np.nan
                    c1, c2 = st.columns(2)
                    c1.metric(f"Upside capture vs {BENCHMARKS[primary_key]['name']}",
                              perc(up_cap) if np.isfinite(up_cap) else "—")
                    c2.metric(f"Downside capture vs {BENCHMARKS[primary_key]['name']}",
                              perc(dn_cap) if np.isfinite(dn_cap) else "—")
                    st.caption("Upside: fund's average up-month / benchmark's average up-month. Downside analogous.")
                else:
                    st.info("Benchmark series empty after alignment.")
            else:
                st.info("Benchmark data not available for capture analysis.")

        # Download (same as before)
        out = pd.DataFrame({"equity": eq})
        out.index.name = "date"
        st.download_button(
            "Download daily equity (CSV)",
            data=out.to_csv().encode("utf-8"),
            file_name=f"{fund_id}_equity_{effective_start}_{effective_end}.csv",
            mime="text/csv",
        )

        st.markdown("---")
        st.subheader("Assumptions & Coverage")
        st.write(f"- Fund: **{FUNDS[fund_id]['name']}**")
        st.write(f"- Effective window: **{effective_start} → {effective_end}** (aligned across tickers)")
        st.write(f"- Rebalance: **{rebalance}**, Annual fee: **{fee:.2%}**")
        st.write("- Prices from Snowflake database. Prototype — not investment advice.")

else:
    st.info("Set your inputs in the sidebar and click **Run backtest**.")