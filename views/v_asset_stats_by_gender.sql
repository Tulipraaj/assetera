CREATE OR REPLACE VIEW v_asset_stats_by_gender AS
SELECT
  c.Gender,
  COUNT(*) AS customers,
  ROUND(MIN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS Min_Assets,
  ROUND(MAX(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Max_Assets,
  ROUND(AVG(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Avg_Assets
FROM customers c
JOIN customer_assets ca ON c.Customer_ID = ca.Customer_ID
GROUP BY Gender
ORDER BY Gender;