"""
Fund definitions and benchmarks - converted from Streamlit app
"""

FUNDS = {
    "F1": {
        "name": "Fund 1 — Core Income (low risk)",
        "allocations": {
            "LQD": 0.50, "IEF": 0.20, "GLD": 0.10, "VEA": 0.10,
            "MSFT": 0.02, "APH": 0.02, "GWW": 0.02, "PH": 0.02, "BSX": 0.02,
        },
        "default_fee": 0.002,  # 0.20%
        "default_rebalance": "Annual",
        "risk_level": "Low",
        "description": "Conservative income-focused portfolio with high allocation to bonds and defensive assets."
    },
    "F2": {
        "name": "Fund 2 — Pro Core (~12% in 10y)",
        "allocations": {
            "LQD": 0.30, "IEF": 0.10, "GLD": 0.08, "VEA": 0.12, "SPY": 0.12,
            "MSFT": 0.03, "APH": 0.03, "GWW": 0.03, "PH": 0.03, "BSX": 0.03, "ETN": 0.03,
            "EME": 0.025, "PWR": 0.025, "FAST": 0.025, "BWXT": 0.025,
        },
        "default_fee": 0.002,
        "default_rebalance": "Annual",
        "risk_level": "Moderate",
        "description": "Balanced portfolio targeting steady growth with moderate risk exposure."
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
        },
        "default_fee": 0.002,
        "default_rebalance": "Annual",
        "risk_level": "Moderate-High",
        "description": "Growth-oriented portfolio with diversified equity exposure and moderate bond allocation."
    },
    "F4": {
        "name": "Fund 4 — Redeem Surge 31 (max RS for >30%)",
        "allocations": {
            "NVDA": 0.24, "AVGO": 0.12, "KLAC": 0.06, "CDNS": 0.06,
            "IDCC": 0.0125, "RDNT": 0.0125, "ACLS": 0.0125, "GPI": 0.0125,
            "VWO": 0.32, "GLD": 0.15,
        },
        "default_fee": 0.002,
        "default_rebalance": "Annual",
        "risk_level": "High",
        "description": "High-growth aggressive portfolio with significant technology and emerging market exposure."
    },
    "F5": {
        "name": "Fund 5 — Bridge Growth 26 (between F3 & F4)",
        "allocations": {
            "NVDA": 0.10, "AVGO": 0.07, "KLAC": 0.06, "CDNS": 0.05, "MSFT": 0.05, "ETN": 0.05,
            "EME": 0.04, "PWR": 0.04, "FAST": 0.025, "BWXT": 0.025,
            "IDCC": 0.06, "RDNT": 0.06, "ACLS": 0.06, "GPI": 0.05, "AGM": 0.05, "TTMI": 0.05,
            "VEA": 0.08, "GLD": 0.04, "VWO": 0.02, "LQD": 0.012, "IEF": 0.008,
        },
        "default_fee": 0.002,
        "default_rebalance": "Annual",
        "risk_level": "High",
        "description": "Bridge portfolio between moderate-high and high risk, balancing growth with some stability."
    },
}

BENCHMARKS = {
    # Singles
    "SPY": {"name": "S&P 500 (SPY)", "definition": {"type": "single", "ticker": "SPY"}},
    "GLD": {"name": "Gold (GLD)", "definition": {"type": "single", "ticker": "GLD"}},
    "VEA": {"name": "Developed ex-US (VEA)", "definition": {"type": "single", "ticker": "VEA"}},
    "VWO": {"name": "Emerging (VWO)", "definition": {"type": "single", "ticker": "VWO"}},
    "IEF": {"name": "UST 7–10y (IEF)", "definition": {"type": "single", "ticker": "IEF"}},
    "LQD": {"name": "US IG Corp (LQD)", "definition": {"type": "single", "ticker": "LQD"}},

    # Blends
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
}
