CREATE OR REPLACE VIEW V_DENORMALIZED AS 

WITH customer_segment_groups AS (
    SELECT 
        c.customer_id,
        -- AGE GROUPS
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
        END AS age_group,
        -- GENDER
        CASE
            WHEN LOWER(TRIM(c.gender)) = 'male' THEN 'Male'
            WHEN LOWER(TRIM(c.gender)) = 'female' THEN 'Female'
            ELSE 'Unknown'
        END AS gender,
        -- MARITAL STATUS
        CASE
            WHEN LOWER(TRIM(c.marital_status)) = 'married' THEN 'Married'
            WHEN LOWER(TRIM(c.marital_status)) = 'single' THEN 'Single'
            WHEN LOWER(TRIM(c.marital_status)) = 'divorced' THEN 'Divorced'
            ELSE 'Unknown'
        END AS marital_status,
        c.number_of_dependents AS dependents,
        -- REGION SEGMENTATION
        CASE
            WHEN c.state IN ('Maine', 'Vermont', 'New Hampshire', 'Massachusetts', 'Connecticut', 'Rhode Island', 'New York', 'New Jersey', 'Pennsylvania') THEN 'Northeast'
            WHEN c.state IN ('Ohio', 'Indiana', 'Illinois', 'Michigan', 'Wisconsin', 'Minnesota', 'Iowa', 'Missouri', 'North Dakota', 'South Dakota', 'Nebraska', 'Kansas') THEN 'Midwest'
            WHEN c.state IN ('Delaware', 'Maryland', 'Virginia', 'West Virginia', 'Kentucky', 'North Carolina', 'South Carolina', 'Tennessee', 'Georgia', 'Florida', 'Alabama', 'Mississippi', 'Arkansas', 'Louisiana', 'Texas', 'Oklahoma') THEN 'South'
            WHEN c.state IN ('Montana', 'Wyoming', 'Colorado', 'New Mexico', 'Arizona', 'Utah', 'Nevada', 'Idaho', 'Washington', 'Oregon', 'California', 'Alaska', 'Hawaii') THEN 'West'
            ELSE 'Unknown'
        END AS region,
        -- ECONOMIC SEGMENTATION
        CASE
            WHEN c.state IN ('California', 'Washington', 'Massachusetts', 'Colorado', 'Connecticut', 'Delaware', 'Virginia', 'Maryland') THEN 'Tech Finance Research'
            WHEN c.state IN ('Michigan', 'Ohio', 'Indiana', 'Illinois', 'Wisconsin', 'Pennsylvania', 'Kentucky', 'Tennessee', 'Alabama', 'Georgia') THEN 'Manufacturing Logistics'
            WHEN c.state IN ('Texas', 'Alaska', 'Oklahoma', 'Louisiana', 'Wyoming', 'Montana', 'Kansas', 'Iowa', 'Nebraska', 'Idaho', 'Arkansas', 'Mississippi') THEN 'Natural Resources Energy'
            WHEN c.state IN ('Florida', 'Nevada', 'Hawaii') THEN 'Tourism Service Driven'
            WHEN c.state IN ('Minnesota', 'Missouri', 'Arizona', 'Oregon', 'Maine', 'Vermont') THEN 'Diversified Balanced'
            ELSE 'Other Unknown'
        END AS economy_class,
        -- INCOME SEGMENTATION
        CASE
            WHEN uiw.INCOME_2023 < 60000 THEN 'Low Income'
            WHEN uiw.INCOME_2023 BETWEEN 60000 AND 69999 THEN 'Middle Income'
            WHEN uiw.INCOME_2023 BETWEEN 70000 AND 79999 THEN 'Upper Middle Income'
            WHEN uiw.INCOME_2023 BETWEEN 80000 AND 89999 THEN 'High Income'
            WHEN uiw.INCOME_2023 >= 90000 THEN 'Very High Income'
            ELSE 'Unknown'
        END AS income_class
    FROM customers c
    LEFT JOIN US_INCOME_WIDE uiw ON TRIM(c.state) = TRIM(uiw.STATE_CLEAN)
    WHERE c.age IS NOT NULL
),
-- Calculate risk profile statistics for each customer
customer_risk_stats AS (
    SELECT 
        ca.customer_id,
        AVG(rp.risk_profile_id::FLOAT) AS avg_risk,
        -- Calculate mode using window functions instead of correlated subquery
        FIRST_VALUE(rp.risk_profile_id) OVER (
            PARTITION BY ca.customer_id 
            ORDER BY COUNT(*) DESC, rp.risk_profile_id ASC
            ROWS UNBOUNDED PRECEDING
        ) AS mode_risk
    FROM customer_answers ca
    JOIN answers a ON ca.question_id = a.question_id AND ca.answer_id = a.answer_id
    JOIN risk_profile rp ON a.risk_profile_id = rp.risk_profile_id
    GROUP BY ca.customer_id, rp.risk_profile_id
),
-- Get the final risk profile calculation
customer_final_risk AS (
    SELECT 
        customer_id,
        CASE
            WHEN mode_risk IN (1,2) THEN FLOOR(avg_risk)
            WHEN mode_risk IN (4,5) THEN CEIL(avg_risk)
            ELSE ROUND(avg_risk)
        END AS final_risk_profile
    FROM (
        SELECT 
            customer_id,
            AVG(avg_risk) AS avg_risk,
            FIRST_VALUE(mode_risk) OVER (PARTITION BY customer_id ORDER BY customer_id) AS mode_risk
        FROM customer_risk_stats
        GROUP BY customer_id, mode_risk
    )
),
cleaned_assets AS (
    SELECT 
        ca.customer_id,
        SUM(CASE
            WHEN ca.total IS NULL OR TRIM(ca.total) = '' THEN 0
            ELSE TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))
        END) AS total_assets
    FROM customer_assets ca
    GROUP BY ca.customer_id
)
SELECT 
    csg.age_group,
    csg.gender,
    csg.marital_status,
    csg.dependents,
    csg.region,
    csg.economy_class,
    csg.income_class,
    COALESCE(cfr.final_risk_profile, 0) AS final_risk_profile,
    COUNT(DISTINCT csg.customer_id) AS customer_count,
    ROUND(SUM(COALESCE(ca.total_assets, 0)), 0) AS total_assets,
    ROUND(AVG(COALESCE(ca.total_assets, 0)), 0) AS avg_assets,
    ROUND(MIN(COALESCE(ca.total_assets, 0)), 0) AS min_assets,
    ROUND(MAX(COALESCE(ca.total_assets, 0)), 0) AS max_assets,
    ROUND(STDDEV(COALESCE(ca.total_assets, 0)), 0) AS std_dev_assets
FROM customer_segment_groups csg
LEFT JOIN cleaned_assets ca ON csg.customer_id = ca.customer_id
LEFT JOIN customer_final_risk cfr ON csg.customer_id = cfr.customer_id
GROUP BY 
    csg.age_group,
    csg.gender,
    csg.marital_status,
    csg.dependents,
    csg.region,
    csg.economy_class,
    csg.income_class,
    cfr.final_risk_profile
ORDER BY 
    csg.age_group,
    csg.gender,
    csg.marital_status,
    csg.dependents,
    csg.region,
    csg.economy_class,
    csg.income_class,
    final_risk_profile;