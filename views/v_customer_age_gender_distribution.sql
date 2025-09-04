CREATE OR REPLACE VIEW v_customer_age_gender_distribution AS
SELECT 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
        WHEN age BETWEEN 60 AND 64 THEN '60-64'
        WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
    END AS age_group,
    COALESCE(Gender, 'Unknown') AS Gender,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM customers c
GROUP BY 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
        WHEN age BETWEEN 60 AND 64 THEN '60-64'
        WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
    END,
    gender
ORDER BY 
    age_group,
    gender,
    customer_count;
