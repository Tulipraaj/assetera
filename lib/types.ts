export interface Customer {
  CUSTOMER_ID: number;
  CONTACT_FIRST_NAME: string;
  CONTACT_LAST_NAME: string;
  STREET: string;
  CITY: string;
  STATE: string;
  ZIP: number;
  COUNTRY: string;
  AGE: number;
  MARITAL_STATUS: string;
  GENDER: string;
  NUMBER_OF_DEPENDENTS: number;
}

export interface FundPrediction {
  CUSTOMER_ID: number;
  FUND_ID: number;
  FINAL_RISK: number;
  AGE: number;
  NUMBER_OF_DEPENDENTS: number;
  MARITAL_STATUS: string;
  GENDER: string;
  TOTAL_ASSETS: number;
}

export interface CustomerWithFunds extends Customer {
  FUND_ID: number;
  FINAL_RISK: number;
  TOTAL_ASSETS: number;
  FUND_PREDICTIONS_FINAL?: {
    FUND_ID: number;
    FINAL_RISK: number;
  };
}

export interface CustomerUpdatePayload {
  CUSTOMER_ID: number;
  FUND_ID: number;
  FINAL_RISK: number;
}
