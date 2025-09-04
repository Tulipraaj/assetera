CREATE OR REPLACE VIEW v_asset_stats_by_region AS
SELECT
  c.Country,
  c.State,
  COUNT(*) AS customers,
  ROUND(MIN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS Min_Assets,
  ROUND(MAX(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Max_Assets,
  ROUND(AVG(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Avg_Assets
FROM customers c
JOIN customer_assets ca ON c.Customer_ID = ca.Customer_ID
GROUP BY Country, State
ORDER BY Country, State;