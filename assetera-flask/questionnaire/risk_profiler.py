"""
Risk profiling questionnaire and mapping to funds
"""
import math
from collections import Counter
import pickle
import numpy as np
import os
# from jsonify

MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'models', 'fund_model.pkl')
class RiskProfiler:
    def __init__(self):
        # self.model_path = 
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
    
    def get_fund_model(self):
        print("this is before loading model")
        with open(MODEL_PATH, 'rb') as f:
            saved_objects = pickle.load(f)
        
        print(saved_objects.keys(), "this is saved obj")
        model = saved_objects["model"]
        print("model1")
        scaler = saved_objects["scaler"]
        print("scaler1")
        label_encoders = saved_objects["label_encoders"]
        print("lab1")
        feature_names = saved_objects["feature_names"]
        print('feature_names1')

        print("returning")

        return model,scaler,label_encoders,feature_names
    
    def calculate_risk_profile(self, responses):
        """Calculate risk score and map to fund"""
        
        risk_profiles = []
        for question in self.questions:
            response_key = f"q_{question['id'].split('_', 1)[1]}"
            if response_key in responses:
                # Find the score for this response
                for option in question['options']:
                    if option['value'] == responses[response_key]:
                        # total_score += option['score']
                        risk_profiles.append(option['score'])
                        break
        
        risk_score = 0
        
        risk_counts = Counter(risk_profiles)
        max_count = max(risk_counts.values())
        most_common_profiles = [profile for profile,count in risk_counts.items() if count==max_count]
        
        most_common_profile_risk = min(most_common_profiles)
        avg_risk = sum(risk_profiles)/len(risk_profiles)

        if most_common_profile_risk in [1,2]:
            risk_score = math.floor(avg_risk)
        elif most_common_profile_risk in [4,5]:
            risk_score = math.ceil(avg_risk)
        
        else:
            risk_score = round(avg_risk)
        

        #         # Map to funds based on risk score
        # if risk_score == 1:
        #     fund = "F1"  # Very conservative
        # elif risk_score == 2:
        #     fund = "F2"  # Conservative to moderate
        # elif risk_score == 3:
        #     fund = "F3"  # Moderate
        # elif risk_score == 4:
        #     fund = "F4"  # Moderate to aggressive
        # else:
        #     fund = "F5"  # Aggressive

        return risk_score
    
    def get_age_bin(self,age):
        bins = [0, 25, 40, 55, 70, 100]
        # np.digitize returns bin index starting from 1, so subtract 1 for 0-based indexing
        bin_index = np.digitize(age, bins, right=False) - 1

        # clamp to last bin if value exceeds
        bin_index = min(bin_index, len(bins) - 2)
        return bin_index
    

    def preprocess_single_input(self,data, label_encoders,scaler,feature_names):
        processed = data.copy()
        print(f"processed is dev {processed}")
        # --- Encode categorical features ---
        for col in ["MARITAL_STATUS", "GENDER"]:
            le = label_encoders[col]

            # Handle unseen values safely
            if data[col] not in le.classes_:
                # temporarily add 'Unknown' if not seen
                le.classes_ = np.append(le.classes_, 'Unknown')
                processed[col] = 'Unknown'
            processed[col] = le.transform([processed[col]])[0]

        # --- Derived features ---
        processed["ASSETS_PER_DEPENDENT"] = processed["TOTAL_ASSETS"] / (processed["NUMBER_OF_DEPENDENTS"] + 1)
        processed["AGE_BINNED"] = self.get_age_bin(processed["AGE"])
        processed["LOG_ASSETS"] = math.log(processed["TOTAL_ASSETS"])

        # --- Arrange in model’s expected order ---
        X = np.array([[processed[f] for f in feature_names]])
        X_scaled = scaler.transform(X)
        return X_scaled


    def map_fund(self,personal_data,responses):
        try:
            
            print("i started")
            model,scaler,label_encoders,feature_names = self.get_fund_model()

            # marital_status = personal_data.get('marital_status')
            # gender = personal_data.get('gender')
            # number_of_dependents = personal_data.get('number_of_dependents')
            # age = personal_data.get('age')
            # total_assets = personal_data.get('total_assets')
            # age_binned = self.get_age_bin(age)
            # assets_per_dependent = total_assets / (number_of_dependents + 1)
            # log_assets = math.log(total_assets)

            # # 3️⃣ Prepare input for model
            # features = np.array([[marital_status, gender, number_of_dependents, age, total_assets]])

            # 4️⃣ Make prediction
            print("im here")
            X_scaled = self.preprocess_single_input(personal_data, label_encoders,scaler,feature_names)
            print("preprocessdeone")
            prediction = model.predict(X_scaled)[0]

            print("modelcal")
            risk = self.calculate_risk_profile(responses)
            # 5️⃣ Return prediction

            print(f"Hellodev this is fund assigned {prediction}")
            return risk , prediction + 1
        


        except Exception as e:
            return {'error': str(e)}

        
