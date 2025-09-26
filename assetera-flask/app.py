from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, date, timedelta
import os
import json
from config import Config
from backtesting.engine import BacktestingEngine
from backtesting.funds import FUNDS, BENCHMARKS
from backtesting.utils import format_kpis
from questionnaire.risk_profiler import RiskProfiler
import logging
from sqlalchemy.dialects.postgresql import JSONB

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)
app.logger.setLevel(logging.DEBUG)

config = Config()
print(f"This is a config message : {config.SNOWFLAKE_USER}")

app.config.from_object(Config)
print(f"Im thop {app.config}")
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
# backtesting_engine = BacktestingEngine()
risk_profiler = RiskProfiler()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Database Models
class User(UserMixin, db.Model):
    __tablename__ = 'users'  # ensure Supabase table name matches
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    risk_profile = db.Column(db.String(10))  # F1, F2, F3, F4, F5
    questionnaire_completed = db.Column(db.Boolean, default=False)
    last_login = db.Column(db.DateTime)

    questionnaire_responses = db.relationship('QuestionnaireResponse', backref='user', lazy=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def get_risk_profile_details(self):
        if self.risk_profile and self.risk_profile in FUNDS:
            return {
                'fund_id': self.risk_profile,
                'fund_name': FUNDS[self.risk_profile]['name'],
                'risk_level': FUNDS[self.risk_profile]['risk_level'],
                'description': FUNDS[self.risk_profile]['description']
            }
        return None

    def get_latest_questionnaire(self):
        return QuestionnaireResponse.query.filter_by(user_id=self.id).order_by(QuestionnaireResponse.created_at.desc()).first()


class QuestionnaireResponse(db.Model):
    __tablename__ = 'questionnaire_responses'  # ensure Supabase table name matches
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    responses = db.Column(JSONB)  # JSONB instead of Text
    risk_score = db.Column(db.Numeric)  # PostgreSQL numeric
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def get_responses_dict(self):
        try:
            return dict(self.responses) if self.responses else {}
        except:
            return {}

# Routes
@app.route('/')
def index():
    """Public landing page with preview of 2 funds"""
    preview_funds = ['F1', 'F2']  # Only show first 2 funds for non-authenticated users
    return render_template('index.html', preview_funds=preview_funds, funds=FUNDS)

@app.route('/preview/<fund_id>')
def fund_preview(fund_id):
    """Public preview of fund performance (limited to F1 and F2)"""
    if fund_id not in ['F1', 'F2']:
        flash('Please sign up to view all fund performances.', 'info')
        return redirect(url_for('signup'))
    
    # Default parameters for preview
    start_date = date.today() - timedelta(days=365*3)  # 3 years
    end_date = date.today()
    
    try:
        backtesting_engine = BacktestingEngine()
        results = backtesting_engine.run_backtest(
            fund_id=fund_id,
            start_date=start_date,
            end_date=end_date,
            start_amount=100000,
            benchmarks=['SPY', '60/40']
        )
        return render_template('fund_preview.html', 
                             fund_id=fund_id, 
                             fund=FUNDS[fund_id],
                             results=results,
                             is_preview=True)
    except Exception as e:
        flash(f'Error generating preview: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        
        if User.query.filter_by(email=email).first():
            flash('Email already registered', 'error')
            return render_template('auth/signup.html')
        
        user = User(email=email)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        
        login_user(user)
        return redirect(url_for('questionnaire'))
    
    return render_template('auth/signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        user = User.query.filter_by(email=email).first()
        
        if user and user.check_password(password):
            user.last_login = datetime.utcnow()
            db.session.commit()
            
            login_user(user)
            if not user.questionnaire_completed:
                return redirect(url_for('questionnaire'))
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid email or password', 'error')
    
    return render_template('auth/login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/questionnaire', methods=['GET', 'POST'])
@login_required
def questionnaire():
    if request.method == 'POST':
        responses = {k: v for k, v in request.form.items() if k.startswith('q_')}

        risk_score, risk_profile = risk_profiler.calculate_risk_profile(responses)

        # Save responses in JSONB
        questionnaire_response = QuestionnaireResponse(
            user_id=current_user.id,
            responses=responses,
            risk_score=risk_score
        )
        db.session.add(questionnaire_response)

        current_user.risk_profile = risk_profile
        current_user.questionnaire_completed = True
        db.session.commit()

        flash(f'Risk assessment complete! You\'ve been matched to {FUNDS[risk_profile]["name"]}', 'success')
        return redirect(url_for('dashboard'))

    questions = risk_profiler.get_questions()
    return render_template('questionnaire.html', questions=questions)

@app.route('/dashboard')
@login_required
def dashboard():
    if not current_user.questionnaire_completed:
        return redirect(url_for('questionnaire'))
    
    # Show all 5 funds for authenticated users
    all_funds = list(FUNDS.keys())
    recommended_fund = current_user.risk_profile
    
    profile_details = current_user.get_risk_profile_details()
    latest_questionnaire = current_user.get_latest_questionnaire()
    
    return render_template('dashboard.html', 
                         all_funds=all_funds, 
                         funds=FUNDS,
                         recommended_fund=recommended_fund,
                         profile_details=profile_details,
                         latest_questionnaire=latest_questionnaire)

@app.route('/profile')
@login_required
def profile():
    """User profile page showing questionnaire history and settings"""
    if not current_user.questionnaire_completed:
        return redirect(url_for('questionnaire'))
    
    # Get all questionnaire responses for this user
    questionnaire_history = QuestionnaireResponse.query.filter_by(
        user_id=current_user.id
    ).order_by(QuestionnaireResponse.created_at.desc()).all()
    
    profile_details = current_user.get_risk_profile_details()
    
    return render_template('profile.html', 
                         profile_details=profile_details,
                         questionnaire_history=questionnaire_history,
                         risk_profiler=risk_profiler)

@app.route('/backtest/<fund_id>')
@login_required
def fund_backtest(fund_id):
    if not current_user.questionnaire_completed:
        return redirect(url_for('questionnaire'))
    
    # Get parameters from query string
    start_date_str = request.args.get('start_date', (date.today() - timedelta(days=365*10)).isoformat())
    end_date_str = request.args.get('end_date', date.today().isoformat())
    start_amount = float(request.args.get('start_amount', 100000))
    benchmarks = request.args.getlist('benchmarks') or ['SPY', '60/40', 'GLD']
    
    try:
        start_date = datetime.fromisoformat(start_date_str).date()
        end_date = datetime.fromisoformat(end_date_str).date()
        backtesting_engine = BacktestingEngine()
        results = backtesting_engine.run_backtest(
            fund_id=fund_id,
            start_date=start_date,
            end_date=end_date,
            start_amount=start_amount,
            benchmarks=benchmarks
        )
        print(f"results fetched {results['pct_pos_months']}")
        return render_template('fund_backtest.html', 
                             fund_id=fund_id, 
                             fund=FUNDS[fund_id],
                             results=results,
                             is_preview=False)
    except Exception as e:
        flash(f'Error running backtest123: {str(e)}', 'error')
        return redirect(url_for('dashboard'))

@app.route('/api/backtest', methods=['POST'])
@login_required
def api_backtest():
    """API endpoint for dynamic backtesting"""
    data = request.get_json()
    
    try:
        backtesting_engine = BacktestingEngine()
        results = backtesting_engine.run_backtest(
            fund_id=data['fund_id'],
            start_date=datetime.fromisoformat(data['start_date']).date(),
            end_date=datetime.fromisoformat(data['end_date']).date(),
            start_amount=data.get('start_amount', 100000),
            benchmarks=data.get('benchmarks', ['SPY'])
        )
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/retake-questionnaire', methods=['GET', 'POST'])
@login_required
def retake_questionnaire():
    if request.method == 'POST':
        responses = {k: v for k, v in request.form.items() if k.startswith('q_')}

        risk_score, risk_profile = risk_profiler.calculate_risk_profile(responses)

        questionnaire_response = QuestionnaireResponse(
            user_id=current_user.id,
            responses=responses,
            risk_score=risk_score
        )
        db.session.add(questionnaire_response)

        old_profile = current_user.risk_profile
        current_user.risk_profile = risk_profile
        db.session.commit()

        if old_profile != risk_profile:
            flash(f'Your risk profile has been updated! You\'re now matched to {FUNDS[risk_profile]["name"]}', 'success')
        else:
            flash(f'Assessment complete! You\'re still matched to {FUNDS[risk_profile]["name"]}', 'info')

        return redirect(url_for('profile'))

    questions = risk_profiler.get_questions()
    return render_template('retake_questionnaire.html', questions=questions)


@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('500.html'), 500

@app.context_processor
def inject_common_data():
    return {
        'funds_count': len(FUNDS),
        'current_year': datetime.now().year
    }

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
