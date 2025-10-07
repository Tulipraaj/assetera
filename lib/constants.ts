export const RISK_LEVELS = {
  1: 'Very Low Risk',
  2: 'Low Risk',
  3: 'Medium Risk',
  4: 'High Risk',
  5: 'Very High Risk',
} as const;

export const FUND_NAMES = {
  1: 'Core Income',
  2: 'Pro Core',
  3: 'Pro Growth 17',
  4: 'Redeem Surge 31',
  5: 'Bridge Growth 26',
} as const;

export type RiskLevel = keyof typeof RISK_LEVELS;
export type FundId = keyof typeof FUND_NAMES;
