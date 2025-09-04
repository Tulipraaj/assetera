
-- View 1.5: Asset Range Analysis Across Customer Segments
CREATE OR REPLACE VIEW v_customer_segment_assets AS
SELECT 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age BETWEEN 50 AND 59 THEN '50-59'
        WHEN age >= 60 THEN 'Above 60'
        ELSE 'Unknown'
    END AS age_segment,
    COALESCE(Gender, 'Unknown') AS gender,
    COALESCE(Marital_Status, 'Unknown') AS marital_status,
    COUNT(*) AS customer_count,
    ROUND(AVG(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS avg_assets,
    ROUND(MIN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS min_assets,
    ROUND(MAX(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS max_assets,
    ROUND(MEDIAN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) AS median_assets
FROM customers c
JOIN customer_assets ca 
    ON c.customer_id = ca.customer_id
GROUP BY 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age BETWEEN 50 AND 59 THEN '50-59'
        WHEN age >= 60 THEN 'Above 60'
        ELSE 'Unknown'
    END,
    COALESCE(Gender, 'Unknown'),
    COALESCE(Marital_Status, 'Unknown')
ORDER BY age_segment, gender, marital_status;
