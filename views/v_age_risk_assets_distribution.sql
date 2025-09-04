use database assetera;
use schema public; 

CREATE OR REPLACE VIEW v_age_risk_assets_distribution AS
WITH age_groups AS (
    SELECT 
        c.customer_id,
        CASE
            WHEN c.age < 30 THEN 'Under 30'
            WHEN c.age BETWEEN 30 AND 34 THEN '30-34'
            WHEN c.age BETWEEN 35 AND 39 THEN '35-39'
            WHEN c.age BETWEEN 40 AND 44 THEN '40-44'
            WHEN c.age BETWEEN 45 AND 49 THEN '45-49'
            WHEN c.age BETWEEN 50 AND 54 THEN '50-54'
            WHEN c.age BETWEEN 55 AND 59 THEN '55-59'
            WHEN c.age BETWEEN 60 AND 64 THEN '60-64'
            WHEN c.age BETWEEN 65 AND 69 THEN '65-69'
            WHEN c.age >= 70 THEN 'Above 70'
            ELSE 'Unknown'
        END AS age_group
    FROM customers 
    WHERE c.age IS NOT NULL
),
cleaned_assets AS (
    SELECT 
        ca.customer_id,
        SUM(
            CASE 
                WHEN ca.total IS NULL OR TRIM(ca.total) = '' THEN 0
                ELSE TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))
            END
        ) AS total_assets
    FROM customer_assets ca
    GROUP BY ca.customer_id
),
risk_profiles AS (
    SELECT 
        c.customer_id,
        get_final_risk_profile(c.customer_id) AS final_risk_profile
    FROM customers c
)
SELECT 
    ag.age_group,
    rp.final_risk_profile,
    COUNT(DISTINCT ag.customer_id) AS customer_count,
    ROUND(SUM(NVL(ca.total_assets,0)), 0) AS total_assets,
    ROUND(AVG(NVL(ca.total_assets,0)), 0) AS avg_assets,
    ROUND(MIN(NVL(ca.total_assets,0)), 0) AS min_assets,
    ROUND(MAX(NVL(ca.total_assets,0)), 0) AS max_assets,
    ROUND(STDDEV(NVL(ca.total_assets,0)), 0) AS std_dev_assets
FROM age_groups ag
JOIN risk_profiles rp 
    ON ag.customer_id = rp.customer_id
LEFT JOIN cleaned_assets ca 
    ON ag.customer_id = ca.customer_id
GROUP BY ag.age_group, rp.final_risk_profile
ORDER BY ag.age_group, rp.final_risk_profile;

