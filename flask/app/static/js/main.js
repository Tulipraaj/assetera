class AssetEraBacktester {
    constructor() {
        this.currentResults = null;
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.setupTabs();
        this.updateFeeValue();
    }
    
    setupEventListeners() {
        // Form submission
        document.getElementById('backtestForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.runBacktest();
        });
        
        // Clear cache
        document.getElementById('clearCache').addEventListener('click', () => {
            this.clearCache();
        });
        
        // Fund selection change - update fee value
        document.getElementById('fund_id').addEventListener('change', () => {
            this.updateFeeValue();
        });
        
        // Fee toggle
        document.getElementById('fee_toggle').addEventListener('change', (e) => {
            document.getElementById('fee_value').disabled = !e.target.checked;
        });
        
        // Benchmark limit (max 3)
        document.querySelectorAll('input[name="benchmarks"]').forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                this.limitBenchmarks();
            });
        });
        
        // Download CSV
        document.getElementById('downloadCsv').addEventListener('click', () => {
            this.downloadCsv();
        });
    }
    
    setupTabs() {
        document.querySelectorAll('.tab-button').forEach(button => {
            button.addEventListener('click', (e) => {
                const tabName = e.target.dataset.tab;
                this.switchTab(tabName);
            });
        });
    }
    
    updateFeeValue() {
        const fundId = document.getElementById('fund_id').value;
        const fundData = window.FUNDS_DATA[fundId];
        if (fundData) {
            document.getElementById('fee_value').value = fundData.default_fee;
        }
    }
    
    limitBenchmarks() {
        const checkboxes = document.querySelectorAll('input[name="benchmarks"]:checked');
        if (checkboxes.length > 3) {
            // Uncheck the last one
            checkboxes[checkboxes.length - 1].checked = false;
            this.showMessage('Maximum 3 benchmarks allowed', 'warning');
        }
    }
    
    async runBacktest() {
        try {
            this.showLoading(true);
            this.hideResults();
            this.hideError();
            
            const formData = this.getFormData();
            
            const response = await fetch('/api/backtest', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(formData)
            });
            
            const result = await response.json();
            
            if (!response.ok) {
                throw new Error(result.error || 'Backtesting failed');
            }
            
            this.currentResults = result;
            this.displayResults(result);
            
        } catch (error) {
            console.error('Backtesting error:', error);
            this.showError(error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    getFormData() {
        const selectedBenchmarks = Array.from(document.querySelectorAll('input[name="benchmarks"]:checked'))
            .map(cb => cb.value);
        
        return {
            fund_id: document.getElementById('fund_id').value,
            start_date: document.getElementById('start_date').value,
            end_date: document.getElementById('end_date').value,
            start_amount: parseFloat(document.getElementById('start_amount').value),
            currency: document.getElementById('currency').value,
            rebalance: document.getElementById('rebalance').value,
            fee_toggle: document.getElementById('fee_toggle').checked,
            fee_value: parseFloat(document.getElementById('fee_value').value),
            rf_rate: parseFloat(document.getElementById('rf_rate').value),
            benchmarks: selectedBenchmarks
        };
    }
    
    displayResults(result) {
        this.displayKPIs(result.kpis);
        this.displayEquityCurve(result);
        this.displayAnalytics(result.analytics);
        this.displayAssumptions(result);
        this.showResults();
        
        if (result.warnings && result.warnings.length > 0) {
            this.showMessage(`Warning: Failed to fetch data for: ${result.warnings.join(', ')}`, 'warning');
        }
        
        if (result.missing_tickers && result.missing_tickers.length > 0) {
            this.showMessage(`Note: These tickers were dropped (weights renormalized): ${result.missing_tickers.join(', ')}`, 'info');
        }
    }
    
    displayKPIs(kpis) {
        const kpiData = [
            { label: 'Final Value', value: kpis.final_value },
            { label: 'Absolute Return', value: kpis.abs_return },
            { label: 'CAGR', value: kpis.cagr },
            { label: 'Volatility (ann.)', value: kpis.vol },
            { label: '% Positive Months', value: kpis.pct_pos_months }
        ];
        
        const container = document.getElementById('kpiCards');
        container.innerHTML = kpiData.map(kpi => `
            <div class="bg-white border border-gray-200 rounded-2xl p-4 shadow-sm">
                <div class="text-sm text-gray-600 mb-1">${kpi.label}</div>
                <div class="text-2xl font-semibold text-slate-900">${kpi.value}</div>
            </div>
        `).join('');
    }
    
    switchTab(tabName) {
        // Update buttons
        document.querySelectorAll('.tab-button').forEach(btn => {
            btn.classList.remove('active', 'border-blue-500', 'text-blue-600');
            btn.classList.add('border-transparent', 'text-gray-500');
        });
        
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active', 'border-blue-500', 'text-blue-600');
        
        // Update content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.add('hidden');
        });
        
        document.getElementById(`${tabName}Tab`).classList.remove('hidden');
    }
    
    showLoading(show) {
        document.getElementById('loadingIndicator').classList.toggle('hidden', !show);
    }
    
    showResults() {
        document.getElementById('results').classList.remove('hidden');
        document.getElementById('welcomeMessage').classList.add('hidden');
    }
    
    hideResults() {
        document.getElementById('results').classList.add('hidden');
        document.getElementById('welcomeMessage').classList.remove('hidden');
    }
    
    showError(message) {
        document.getElementById('errorText').textContent = message;
        document.getElementById('errorMessage').classList.remove('hidden');
    }
    
    hideError() {
        document.getElementById('errorMessage').classList.add('hidden');
    }
    
    showMessage(message, type = 'info') {
        // Create temporary message element
        const messageEl = document.createElement('div');
        const bgColor = type === 'warning' ? 'bg-yellow-50 border-yellow-200 text-yellow-700' : 
                       type === 'error' ? 'bg-red-50 border-red-200 text-red-700' :
                       'bg-blue-50 border-blue-200 text-blue-700';
        
        messageEl.className = `mb-4 p-4 ${bgColor} border rounded-lg`;
        messageEl.textContent = message;
        
        document.querySelector('#results').insertBefore(messageEl, document.querySelector('#results').firstChild);
        
        // Auto remove after 5 seconds
        setTimeout(() => messageEl.remove(), 5000);
    }
    
    async clearCache() {
        try {
            const response = await fetch('/api/clear-cache', { method: 'POST' });
            this.showMessage('Cache cleared. Rerun the backtest.', 'info');
        } catch (error) {
            console.error('Clear cache error:', error);
        }
    }
    
    downloadCsv() {
        if (!this.currentResults) return;
        
        const csvData = this.generateCsvData();
        const blob = new Blob([csvData], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${this.currentResults.fund_name.replace(/[^a-zA-Z0-9]/g, '_')}_equity_${this.currentResults.effective_start}_${this.currentResults.effective_end}.csv`;
        a.click();
        window.URL.revokeObjectURL(url);
    }
    
    generateCsvData() {
        const portfolio = this.currentResults.portfolio;
        let csv = 'date,equity\n';
        
        for (let i = 0; i < portfolio.dates.length; i++) {
            csv += `${portfolio.dates[i]},${portfolio.equity[i]}\n`;
        }
        
        return csv;
    }
    
    displayAssumptions(result) {
        const assumptions = `
            <div class="space-y-2 text-sm text-slate-700">
                <div>• Fund: <strong>${result.fund_name}</strong></div>
                <div>• Effective window: <strong>${result.effective_start} → ${result.effective_end}</strong> (aligned across tickers)</div>
                <div>• Rebalance: <strong>${document.getElementById('rebalance').value}</strong>, Annual fee: <strong>${(parseFloat(document.getElementById('fee_value').value) * 100).toFixed(2)}%</strong></div>
                <div>• Prices via Yahoo Finance (auto-adjusted 'Close'). Prototype — not investment advice.</div>
            </div>
        `;
        document.getElementById('assumptionsContent').innerHTML = assumptions;
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    // Make funds data available globally (will be injected by template)
    window.FUNDS_DATA = {{ funds|tojsonhtml }};
    
    // Initialize the application
    window.backtester = new AssetEraBacktester();
});