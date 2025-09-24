"""
Utility functions for formatting and display
"""

def money(x, cur="USD"):
    """Format money values"""
    sym = "$" if cur.upper() == "USD" else "₹"
    try: 
        return f"{sym}{x:,.0f}"
    except Exception: 
        return f"{sym}{x}"

def perc(x):
    """Format percentage values"""
    try: 
        return f"{x * 100:.2f}%"
    except Exception: 
        return "—"

def format_kpis(kpis, start_amount, currency="USD"):
    """Format KPIs for display"""
    if not kpis:
        return {}
    
    return {
        'final_value': money(kpis['final_value'], currency),
        'abs_return': perc(kpis['abs_return']),
        'cagr': perc(kpis['cagr']) if np.isfinite(kpis['cagr']) else "—",
        'vol': perc(kpis['vol']) if np.isfinite(kpis['vol']) else "—",
        'sharpe': f"{kpis['sharpe']:.2f}" if np.isfinite(kpis['sharpe']) else "—"
    }

import numpy as np
