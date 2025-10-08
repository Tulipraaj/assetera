export const RISK_LEVELS = {
  1: 'Low Risk',
  2: 'Below Average Tolerance',
  3: 'Average Tolerance',
  4: 'Above Average Tolerance',
  5: 'High Tolerance',
} as const;

export const FUND_NAMES = {
  1: 'Shield Fund',
  2: 'Core Fund',
  3: 'Pulse 17 Fund',
  4: 'Vertex 26  Fund',
  5: 'Alpha 31 Fund',
} as const;

export type RiskLevel = keyof typeof RISK_LEVELS;
export type FundId = keyof typeof FUND_NAMES;
 
