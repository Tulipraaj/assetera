# AssetEra - Fund Backtesting Platform

A Flask-based web application for backtesting investment fund performance, converted from a Streamlit prototype. The platform allows users to analyze historical performance of 5 different investment funds with personalized risk assessment and recommendations.

## Features

### Public Access
- **Fund Previews**: View limited 3-year performance data for 2 funds (F1 and F2) without registration
- **Landing Page**: Overview of available funds and platform features

### Authenticated Users
- **Risk Assessment**: Complete a comprehensive questionnaire to determine investment risk profile
- **Personalized Recommendations**: Get matched to the most suitable fund based on risk tolerance
- **Full Backtesting**: Access up to 10 years of historical performance data for all 5 funds
- **Interactive Charts**: Plotly-powered visualizations for performance analysis
- **Profile Management**: View assessment history and retake questionnaires
- **Comprehensive Analytics**: Rolling returns, yearly performance, distribution analysis, and risk metrics

## Fund Portfolio

- **F1 - Core Income**: Low-risk, income-focused portfolio (50% bonds, defensive assets)
- **F2 - Pro Core**: Moderate-risk, balanced growth portfolio (~12% target return)
- **F3 - Pro Growth 17**: Moderate-high risk, growth-oriented portfolio
- **F4 - Redeem Surge 31**: High-risk, aggressive growth portfolio (>30% growth potential)
- **F5 - Bridge Growth 26**: High-risk bridge between F3 and F4

## Technology Stack

- **Backend**: Flask, SQLAlchemy, Flask-Login
- **Frontend**: HTML5, Tailwind CSS, JavaScript
- **Data Visualization**: Plotly.js
- **Data Processing**: Pandas, NumPy
- **Market Data**: Yahoo Finance API
- **Database**: SQLite (development), PostgreSQL (production ready)

## Installation

1. **Clone the repository**
   \`\`\`bash
   git clone <repository-url>
   cd assetera-flask
   \`\`\`

2. **Create virtual environment**
   \`\`\`bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   \`\`\`

3. **Install dependencies**
   \`\`\`bash
   pip install -r requirements.txt
   \`\`\`

4. **Set environment variables**
   \`\`\`bash
   export SECRET_KEY="your-secret-key-here"
   export DATABASE_URL="sqlite:///assetera.db"  # or PostgreSQL URL for production
   \`\`\`

5. **Initialize database**
   \`\`\`bash
   python -c "from app import app, db; app.app_context().push(); db.create_all()"
   \`\`\`

6. **Run the application**
   \`\`\`bash
   python app.py
   \`\`\`

   Or for production:
   \`\`\`bash
   python run.py
   \`\`\`

## Usage

### For Visitors
1. Visit the homepage to see fund previews
2. Click "View Performance" on F1 or F2 to see limited backtesting data
3. Sign up to access full features

### For Registered Users
1. **Sign Up**: Create an account with email and password
2. **Risk Assessment**: Complete the 7-question risk profiler
3. **Get Recommendation**: Receive personalized fund recommendation
4. **Explore Dashboard**: View all 5 funds and their characteristics
5. **Analyze Performance**: Click on any fund to see detailed backtesting
6. **Customize Analysis**: Adjust date ranges, starting amounts, and benchmarks
7. **Profile Management**: View assessment history and retake questionnaire

## API Endpoints

- `GET /` - Landing page with fund previews
- `GET /preview/<fund_id>` - Public fund preview (F1, F2 only)
- `POST /signup` - User registration
- `POST /login` - User authentication
- `GET /questionnaire` - Risk assessment form
- `POST /questionnaire` - Process risk assessment
- `GET /dashboard` - User dashboard (authenticated)
- `GET /backtest/<fund_id>` - Fund performance analysis
- `GET /profile` - User profile and settings
- `POST /retake-questionnaire` - Update risk profile
- `POST /api/backtest` - Dynamic backtesting API

## Risk Assessment

The platform uses a 7-factor risk assessment model:

1. **Age Group** - Investment time horizon
2. **Income Level** - Financial capacity
3. **Investment Experience** - Knowledge and comfort level
4. **Time Horizon** - Investment duration goals
5. **Risk Tolerance** - Reaction to market volatility
6. **Investment Goals** - Primary objectives
7. **Volatility Comfort** - Acceptance of fluctuations

Scores are normalized and mapped to funds:
- 0-20%: F1 (Conservative)
- 21-40%: F2 (Moderate)
- 41-60%: F3 (Moderate-High)
- 61-80%: F5 (Bridge)
- 81-100%: F4 (Aggressive)

## Performance Metrics

The backtesting engine calculates:

- **Total Return**: Absolute performance over period
- **CAGR**: Compound Annual Growth Rate
- **Volatility**: Annualized standard deviation
- **Sharpe Ratio**: Risk-adjusted returns
- **VaR/CVaR**: Value at Risk metrics
- **Rolling Returns**: 12-month trailing performance
- **Monthly Distribution**: Return frequency analysis
- **Benchmark Comparison**: Performance vs market indices

## Security Features

- Password hashing with Werkzeug
- Session management with Flask-Login
- CSRF protection ready
- SQL injection prevention with SQLAlchemy ORM
- Input validation and sanitization

## Production Deployment

For production deployment:

1. **Use PostgreSQL**:
   \`\`\`bash
   export DATABASE_URL="postgresql://user:password@localhost/assetera"
   \`\`\`

2. **Use Gunicorn**:
   \`\`\`bash
   pip install gunicorn
   gunicorn -w 4 -b 0.0.0.0:8000 run:app
   \`\`\`

3. **Set secure secret key**:
   \`\`\`bash
   export SECRET_KEY=$(python -c 'import secrets; print(secrets.token_hex())')
   \`\`\`

4. **Configure reverse proxy** (Nginx recommended)

## Disclaimer

This platform is for educational and research purposes only. Historical performance does not guarantee future results. The risk assessments and fund recommendations do not constitute investment advice. Always consult with a qualified financial advisor before making investment decisions.

## License

This project is proprietary software developed for AssetEra. All rights reserved.
