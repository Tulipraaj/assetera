# Sentiment Analysis

## Project Overview
This project focuses on analyzing the impact of financial news sentiment on stock price movements.  
We combine **news data** with **market OHLCV data** to predict whether stock prices will go **up (1)** or **down (0)** on the next day.

---

## Dataset
- **Outsourced Data**: [FNSPID Dataset (HuggingFace)](https://huggingface.co/datasets/Zihan1004/FNSPID)  
- Contains large-scale financial news and stock price information.  
- Used as the foundation for sentiment classification and predictive modeling.

---

## Methodology

### 1. Sentiment Extraction
- Applied **FinBERT** (finance-domain BERT) to classify news into **positive, negative, neutral** categories.  
- Aggregated daily ticker-wise sentiment scores with weights based on probability.

### 2. Feature Engineering
- Combined **sentiment features** with **stock OHLCV data**.  
- Generated lagged features to capture temporal effects.

### 3. Models Tested
- Logistic Regression  
- ARIMA / ARIMAX  


### 4. Results
- **Reddit dataset** → max accuracy ~58%.  
- **Extended DJIA dataset (2011–2017, 30 tickers)** →  
  - ARIMAX Accuracy: **67.3%**  
  - Logistic Regression Accuracy: **76.6%** 

---

## Key Takeaways
- Logistic Regression performed the best with **76.6% accuracy**.   
- FinBERT sentiment scores significantly improved prediction compared to raw text.

---

