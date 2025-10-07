export interface Customer {
  CUSTOMER_ID: number;
  NAME: string;
  AGE: number;
  GENDER: string;
  MARITAL_STATUS: string;
  FINAL_RISK: 'Low' | 'Medium' | 'High';
  FUND_ID: string;
  INVESTMENT_AMOUNT: number;
}

export interface CustomerUpdatePayload {
  CUSTOMER_ID: number;
  FINAL_RISK: 'Low' | 'Medium' | 'High';
  FUND_ID: string;
}
