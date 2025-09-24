from flask import Blueprint, render_template, request, jsonify
from datetime import datetime, date, timedelta
import json
import pandas as pd
import numpy as np

from app.config import Config
from app.services.yahoo_finance_service import YahooFinanceService
from app.services.backtesting_service import BacktestingService

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Main backtesting interface"""
    funds = Config.FUNDS
    benchmarks = Config.BENCHMARKS
    
    # Default values
    today = date.today()
    default_start = date(today.year - 10, today.month, today.day)
    
    default_config = {
        'fund_id': 'F1',
        'start_date': default_start.isoformat(),
        'end_date': today.isoformat(),
        'start_amount': 100000,
        'currency': 'USD',
        'rebalance': 'Annual',
        'fee_toggle': True,
        'rf_rate': 0.0,
        'benchmarks': ['SPY', '60/40', 'GLD']
    }
    
    return render_template('index.html', 
                         funds=funds, 
                         benchmarks=benchmarks,
                         config=default_config)

@main_bp.route('/api/backtest', methods=['POST'])
def run_backtest():
    """Run backtesting calculation"""
    
    try:
        data = request.json
        
        # Extract parameters
        fund_id = data.get('fund_id', 'F1')
        start_date = datetime.strptime(data.get('start_date'), '%Y-%m-%d').date()
        end_date = datetime.strptime(data.get('end_date'), '%Y-%m-%d').date()
        start_amount = float(data.get('start_amount', 100000))
        currency = data.get('currency', 'USD')
        rebalance = data.get('rebalance', 'Annual')
        fee_toggle = data.get('fee_toggle', True)
        fee_value = float(data.get('fee_value', 0.002))
        rf_rate = float(data.get('rf_rate', 0.0))
        selected_benchmarks = data.get('benchmarks', ['SPY'])
        
        # Clamp end date
        end_date = min(end_date, date.today())
        
        if start_date >= end_date:
            return jsonify({'error': 'Start date must be before end date'}), 400
        
        # Initialize services
        yahoo_service = YahooFinanceService()
        backtesting_service = BacktestingService()
        
        # Get fund configuration
        if fund_id not in Config.FUNDS:
            return jsonify({'error': 'Invalid fund ID'}), 400
        
        target_weights = Config.FUNDS[fund_id]["allocations"].copy()
        fund_tickers = set(target_weights.keys())
        
        # Get benchmark tickers
        bench_tickers = set()
        for b in selected_benchmarks:
            if b in Config.BENCHMARKS:
                d = Config.BENCHMARKS[b]["definition"]
                if d["type"] == "single": 
                    bench_tickers.add(d["ticker"])
                else: 
                    bench_tickers.update(d["weights"].keys())
        
        # Fetch all needed tickers
        needed = sorted(fund_tickers | bench_tickers)
        fetch_result = yahoo_service.fetch_prices(needed, start_date, end_date)
        
        if not fetch_result['success']:
            return jsonify({'error': fetch_result['message']}), 500
        
        prices = fetch_result['data']
        
        # Check fund ticker availability
        present = [t for t in fund_tickers if t in prices.columns]
        missing = sorted(list(fund_tickers - set(present)))
        
        if not present:
            return jsonify({'error': 'None of the fund tickers have data for the selected range'}), 400
        
        # Renormalize weights for available tickers
        weights = backtesting_service.renormalize_weights(target_weights, present)
        
        # Calculate returns
        returns_all = backtesting_service.compute_daily_returns(prices)
        returns_portfolio = returns_all[present].dropna(how="any")
        
        if returns_portfolio.empty or len(returns_portfolio) < 2:
            return jsonify({'error': 'Not enough overlapping data for the selected dates'}), 400
        
        # Calculate portfolio equity
        fee = float(fee_value) if fee_toggle else 0.0
        equity = backtesting_service.iterate_portfolio_path(
            returns_portfolio, weights, rebalance=rebalance, fee_annual=fee
        )
        
        if equity.empty:
            return jsonify({'error': 'Portfolio series is empty after alignment'}), 400
        
        # Normalize equity to start at 1
        equity = equity / equity.iloc[0]
        effective_start = equity.index[0].date()
        effective_end = equity.index[-1].date()
        
        # Calculate KPIs
        kpis = backtesting_service.calculate_kpis(
            equity, start_amount, effective_start, effective_end, rf_annual=rf_rate
        )
        
        # Prepare benchmark data
        benchmark_data = []
        for b in selected_benchmarks:
            if b in Config.BENCHMARKS:
                bench_series = backtesting_service.mix_benchmark(returns_all, Config.BENCHMARKS[b]["definition"])
                if not bench_series.empty:
                    bench_series = bench_series.reindex(equity.index).ffill().dropna()
                    if not bench_series.empty:
                        bench_series = bench_series / bench_series.iloc[0]
                        benchmark_data.append({
                            'name': Config.BENCHMARKS[b]["name"],
                            'data': bench_series.tolist(),
                            'dates': bench_series.index.strftime('%Y-%m-%d').tolist()
                        })
        
        # Prepare response
        result = {
            'success': True,
            'fund_name': Config.FUNDS[fund_id]['name'],
            'effective_start': effective_start.isoformat(),
            'effective_end': effective_end.isoformat(),
            'portfolio': {
                'equity': equity.tolist(),
                'dates': equity.index.strftime('%Y-%m-%d').tolist()
            },
            'benchmarks': benchmark_data,
            'kpis': {
                'final_value': backtesting_service.format_money(kpis['final_value'], currency),
                'abs_return': backtesting_service.format_percent(kpis['abs_return']),
                'cagr': backtesting_service.format_percent(kpis['cagr']) if np.isfinite(kpis['cagr']) else "—",
                'vol': backtesting_service.format_percent(kpis['vol']) if np.isfinite(kpis['vol']) else "—",
                'pct_pos_months': backtesting_service.format_percent(kpis['pct_pos_months']) if np.isfinite(kpis['pct_pos_months']) else "—"
            },
            'analytics': {
                'yearly_returns': backtesting_service.yearly_returns(equity).to_dict() if not backtesting_service.yearly_returns(equity).empty else {},
                'monthly_returns': backtesting_service.monthly_returns(equity).values.tolist() if not backtesting_service.monthly_returns(equity).empty else [],
                'rolling_12m': {
                    'data': backtesting_service.rolling_12m_returns(equity).tolist(),
                    'dates': backtesting_service.rolling_12m_returns(equity).index.strftime('%Y-%m-%d').tolist()
                } if not backtesting_service.rolling_12m_returns(equity).empty else {'data': [], 'dates': []}
            },
            'missing_tickers': missing,
            'warnings': fetch_result.get('failed_tickers', [])
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@main_bp.route('/api/funds')
def get_funds():
    """Get available funds"""
    return jsonify(Config.FUNDS)

@main_bp.route('/api/benchmarks')
def get_benchmarks():
    """Get available benchmarks"""
    return jsonify(Config.BENCHMARKS)