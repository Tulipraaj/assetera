"""
Risk profiling questionnaire and mapping to funds
"""

class RiskProfiler:
    def __init__(self):
        self.questions = [
            {
                "id": "q_Years Experience",
                "question": "How many years from now do you expect to begin taking income from your investments?",
                "type": "radio",
                "options": [
                    {"value": "1-5", "text": "1-5 years", "score": 1},
                    {"value": "5-10", "text": "5-10 years", "score": 2},
                    {"value": "10-15", "text": "10-15 years", "score": 3},
                    {"value": "15-20", "text": "15-20 years", "score": 4},
                    {"value": "20+", "text": "20+ years", "score": 5}
                ]
            },
            {
                "id": "q_Initial Loss Behavior",
                "question": r"Choose the BEST match to your feelings on the following situation: Your account value after rising 15% in the previous quarter is down 20% this quarter.",
                "type": "radio",
                "options": [
                    {"value": "Doesn’t really matter, I’m in it for the long haul", "text": "Doesn’t really matter, I’m in it for the long haul", "score": 4},
                    {"value": "I’m reaching for the phone to call my advisor", "text": "I’m reaching for the phone to call my advisor", "score": 2},
                    {"value": "Sell it all, cash is the place for me", "text": "Sell it all, cash is the place for me", "score": 1},
                    {"value": "I feel a little nervous about it", "text": "I feel a little nervous about it", "score": 3},
                    {"value": "Buy more!", "text": "Buy more!", "score": 5}
                ]
            },
            {
                "id": "q_Subsequent Loss Behavior",
                "question": r"What if your portfolio’s value declined another 10% in the subsequent quarter?",
                "type": "radio",
                "options": [
                    {"value": "Sell it all and move to safety", "text": "Sell it all and move to safety", "score": 1},
                    {"value": "Divest the declining positions", "text": "Divest the declining positions", "score": 3},
                    {"value": "Wait it out/Do nothing", "text": "Wait it out/Do nothing", "score": 4},
                    {"value": "Invest in the positions that have declined the most", "text": "Invest in the positions that have declined the most", "score": 5}
                ]
            },
            {
                "id": "q_Retirement Account Role",
                "question": "What role does your retirement account play in your overall savings strategy? Choose the best answer for you:",
                "type": "radio",
                "options": [
                    {"value": "It’s all I have", "text": "It’s all I have", "score": 1},
                    {"value": "I have an emergency fund as well", "text": "I have an emergency fund as well", "score": 2},
                    {"value": "It’s a piece of the picture, I also have real estate", "text": "It’s a piece of the picture, I also have real estate", "score": 2},
                    {"value": "Who has the funds to save for retirement?", "text": "Who has the funds to save for retirement?", "score": 3}
                ]
            },
            {
                "id": "q_Income Prediction",
                "question": "What do you expect your total income to do over the next 10-15 years?",
                "type": "radio",
                "options": [
                    {"value": "Increase significantly", "text": "Increase significantly", "score": 5},
                    {"value": "Improve", "text": "Improve", "score": 3},
                    {"value": "Stay the same", "text": "Stay the same", "score": 3},
                    {"value": "Decease", "text": "Decease", "score": 2},
                    {"value": "Decline substantially", "text": "Decline substantially", "score": 1}
                ]
            },
            {
                "id": "q_Reward vs. Risk",
                "question": "If given the opportunity to improve your returns by selecting investments whose value may fluctuate significantly over time, would you:",
                "type": "radio",
                "options": [
                    {"value": "Not likely to take on more risk", "text": "Not likely to take on more risk", "score": 2},
                    {"value": "Take a little risk with a small portion of the portfolio", "text": "Take a little risk with a small portion of the portfolio", "score": 3},
                    {"value": "Take significantly more risk with some of the portfolio", "text": "Take significantly more risk with some of the portfolio", "score": 4},
                    {"value": "Take a lot more risk with the whole portfolio", "text": "Take a lot more risk with the whole portfolio", "score": 5}
                ]
            },
            {
                "id": "q_Monitoring Behavior",
                "question": "When it comes to keeping track of your investments, which statement below BEST describes you?",
                "type": "radio",
                "options": [
                    {"value": "I don’t monitor my investment accounts and I don’t have them reviewed", "text": "I don’t monitor my investment accounts and I don’t have them reviewed", "score": 5},
                    {"value": "I take a look at the statement every once in a while", "text": "I take a look at the statement every once in a while", "score": 4},
                    {"value": "I review my statements regularly", "text": "I review my statements regularly", "score": 3},
                    {"value": "I review my statements regularly and have my portfolio reviewed annually", "text": "I review my statements regularly and have my portfolio reviewed annually", "score": 2}
                ]
            },
            {
                "id": "q_Experience",
                "question": "How would you describe your investment experience?",
                "type": "radio",
                "options": [
                    {"value": "Little to no experience", "text": "Little to no experience", "score": 2},
                    {"value": "Some experience, but only in mutual funds", "text": "Some experience, but only in mutual funds", "score": 3},
                    {"value": "Experienced in mutual funds and some individual stocks and bonds", "text": "Experienced in mutual funds and some individual stocks and bonds", "score": 4},
                    {"value": "Vast knowledge and experience with many types of investments", "text": "Vast knowledge and experience with many types of investments", "score": 5}
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
