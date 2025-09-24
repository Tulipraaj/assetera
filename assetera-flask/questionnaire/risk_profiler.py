"""
Risk profiling questionnaire and mapping to funds
"""

class RiskProfiler:
    def __init__(self):
        self.questions = [
            {
                "id": "q_age",
                "question": "What is your age group?",
                "type": "radio",
                "options": [
                    {"value": "18-25", "text": "18-25 years", "score": 5},
                    {"value": "26-35", "text": "26-35 years", "score": 4},
                    {"value": "36-45", "text": "36-45 years", "score": 3},
                    {"value": "46-55", "text": "46-55 years", "score": 2},
                    {"value": "55+", "text": "55+ years", "score": 1}
                ]
            },
            {
                "id": "q_income",
                "question": "What is your annual household income?",
                "type": "radio",
                "options": [
                    {"value": "below-50k", "text": "Below $50,000", "score": 1},
                    {"value": "50k-100k", "text": "$50,000 - $100,000", "score": 2},
                    {"value": "100k-200k", "text": "$100,000 - $200,000", "score": 3},
                    {"value": "200k-500k", "text": "$200,000 - $500,000", "score": 4},
                    {"value": "above-500k", "text": "Above $500,000", "score": 5}
                ]
            },
            {
                "id": "q_investment_experience",
                "question": "How would you describe your investment experience?",
                "type": "radio",
                "options": [
                    {"value": "beginner", "text": "Beginner - Little to no experience", "score": 1},
                    {"value": "some", "text": "Some experience with basic investments", "score": 2},
                    {"value": "moderate", "text": "Moderate - Familiar with various investment types", "score": 3},
                    {"value": "experienced", "text": "Experienced - Active investor for several years", "score": 4},
                    {"value": "expert", "text": "Expert - Professional or very experienced", "score": 5}
                ]
            },
            {
                "id": "q_time_horizon",
                "question": "What is your investment time horizon?",
                "type": "radio",
                "options": [
                    {"value": "short", "text": "Less than 2 years", "score": 1},
                    {"value": "medium-short", "text": "2-5 years", "score": 2},
                    {"value": "medium", "text": "5-10 years", "score": 3},
                    {"value": "long", "text": "10-20 years", "score": 4},
                    {"value": "very-long", "text": "More than 20 years", "score": 5}
                ]
            },
            {
                "id": "q_risk_tolerance",
                "question": "If your investment lost 20% of its value in a month, what would you do?",
                "type": "radio",
                "options": [
                    {"value": "sell-all", "text": "Sell all investments immediately", "score": 1},
                    {"value": "sell-some", "text": "Sell some to reduce risk", "score": 2},
                    {"value": "hold", "text": "Hold and wait for recovery", "score": 3},
                    {"value": "buy-more", "text": "Buy more at the lower price", "score": 4},
                    {"value": "excited", "text": "Get excited about the buying opportunity", "score": 5}
                ]
            },
            {
                "id": "q_portfolio_goal",
                "question": "What is your primary investment goal?",
                "type": "radio",
                "options": [
                    {"value": "preservation", "text": "Capital preservation", "score": 1},
                    {"value": "income", "text": "Generate steady income", "score": 2},
                    {"value": "balanced", "text": "Balanced growth and income", "score": 3},
                    {"value": "growth", "text": "Long-term growth", "score": 4},
                    {"value": "aggressive", "text": "Aggressive growth", "score": 5}
                ]
            },
            {
                "id": "q_volatility_comfort",
                "question": "How comfortable are you with portfolio volatility?",
                "type": "radio",
                "options": [
                    {"value": "very-low", "text": "I prefer minimal fluctuations", "score": 1},
                    {"value": "low", "text": "Small fluctuations are acceptable", "score": 2},
                    {"value": "moderate", "text": "Moderate fluctuations for better returns", "score": 3},
                    {"value": "high", "text": "High volatility is fine for growth potential", "score": 4},
                    {"value": "very-high", "text": "I thrive on high volatility investments", "score": 5}
                ]
            }
        ]
    
    def get_questions(self):
        return self.questions
    
    def calculate_risk_profile(self, responses):
        """Calculate risk score and map to fund"""
        total_score = 0
        max_possible_score = len(self.questions) * 5
        
        for question in self.questions:
            response_key = f"q_{question['id'].split('_', 1)[1]}"
            if response_key in responses:
                # Find the score for this response
                for option in question['options']:
                    if option['value'] == responses[response_key]:
                        total_score += option['score']
                        break
        
        # Normalize to 0-1 scale
        risk_score = total_score / max_possible_score
        
        # Map to funds based on risk score
        if risk_score <= 0.2:
            fund = "F1"  # Very conservative
        elif risk_score <= 0.4:
            fund = "F2"  # Conservative to moderate
        elif risk_score <= 0.6:
            fund = "F3"  # Moderate
        elif risk_score <= 0.8:
            fund = "F5"  # Moderate to aggressive
        else:
            fund = "F4"  # Aggressive
        
        return risk_score, fund
