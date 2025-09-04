CREATE OR REPLACE VIEW v_asset_stats_by_age AS
SELECT 
    CASE 
        WHEN c.Age < 30 THEN 'Under 30'
        WHEN c.Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN c.Age BETWEEN 40 AND 49 THEN '40-49'
        WHEN c.Age BETWEEN 50 AND 59 THEN '50-59'
        ELSE 'Above 60'
    END AS Age_Group,
    ROUND(MIN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS Min_Assets,
    ROUND(MAX(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Max_Assets,
    ROUND(AVG(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS  Avg_Assets
FROM customers c
JOIN customer_assets ca ON c.Customer_ID = ca.Customer_ID
GROUP BY   
    CASE 
        WHEN c.Age < 30 THEN 'Under 30'
        WHEN c.Age BETWEEN 30 AND 39 THEN '30-39'
        WHEN c.Age BETWEEN 40 AND 49 THEN '40-49'
        WHEN c.Age BETWEEN 50 AND 59 THEN '50-59'
        ELSE 'Above 60'
    END ;
