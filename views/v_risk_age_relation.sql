CREATE OR REPLACE VIEW v_risk_age_relation AS
WITH customer_risk_counts AS (
    SELECT
        c.Customer_ID,
        c.Age,
        a.Risk_Profile_ID,
        COUNT(*) AS Risk_Count
    FROM customers c
    JOIN customer_answers ca ON c.Customer_ID = ca.Customer_ID
    JOIN answers a ON ca.Question_ID = a.Question_ID AND ca.Answer_ID = a.Answer_ID
    WHERE c.Age IS NOT NULL AND a.Risk_Profile_ID IS NOT NULL
    GROUP BY c.Customer_ID, c.Age, a.Risk_Profile_ID
),
customer_stats AS (
    SELECT
        c.Customer_ID,
        c.Age,
        AVG(a.Risk_Profile_ID) AS Avg_Risk_Profile,
        MODE() WITHIN GROUP (ORDER BY a.Risk_Profile_ID) AS Mode_Risk_Profile
    FROM customers c
    JOIN customer_answers ca ON c.Customer_ID = ca.Customer_ID
    JOIN answers a ON ca.Question_ID = a.Question_ID AND ca.Answer_ID = a.Answer_ID
    WHERE c.Age IS NOT NULL AND a.Risk_Profile_ID IS NOT NULL
    GROUP BY c.Customer_ID, c.Age
),
customer_final AS (
    SELECT
        Customer_ID,
        Age,
        CASE
            WHEN Mode_Risk_Profile IN (1,2) THEN FLOOR(Avg_Risk_Profile)
            WHEN Mode_Risk_Profile IN (4,5) THEN CEIL(Avg_Risk_Profile)
            ELSE ROUND(Avg_Risk_Profile)
        END AS Final_Risk_Profile_ID
    FROM customer_stats
),
customer_labeled AS (
    SELECT
        cf.Customer_ID,
        CASE
            WHEN cf.Age < 30 THEN 'Under 30'
            WHEN cf.Age BETWEEN 30 AND 34 THEN '30-34'
            WHEN cf.Age BETWEEN 35 AND 39 THEN '35-39'
            WHEN cf.Age BETWEEN 40 AND 44 THEN '40-44'
            WHEN cf.Age BETWEEN 45 AND 49 THEN '45-49'
            WHEN cf.Age BETWEEN 50 AND 54 THEN '50-54'
            WHEN cf.Age BETWEEN 55 AND 59 THEN '55-59'
            WHEN cf.Age BETWEEN 60 AND 64 THEN '60-64'
            WHEN cf.Age BETWEEN 65 AND 69 THEN '65-69'
            WHEN cf.Age >= 70 THEN 'Above 70'
        END AS Age_Group,
        cf.Final_Risk_Profile_ID,
        rp.Risk_Profile
    FROM customer_final cf
    JOIN risk_profile rp ON cf.Final_Risk_Profile_ID = rp.Risk_Profile_ID
)
SELECT
    Age_Group,
    Final_Risk_Profile_ID,
    Risk_Profile AS Final_Risk_Profile,
    COUNT(DISTINCT Customer_ID) AS Customer_Count
FROM customer_labeled
GROUP BY Age_Group, Final_Risk_Profile_ID, Risk_Profile
ORDER BY 
    CASE Age_Group
        WHEN 'Under 30' THEN 1
        WHEN '30-34' THEN 2
        WHEN '35-39' THEN 3
        WHEN '40-44' THEN 4
        WHEN '45-49' THEN 5
        WHEN '50-54' THEN 6
        WHEN '55-59' THEN 7
        WHEN '60-64' THEN 8
        WHEN '65-69' THEN 9
        WHEN 'Above 70' THEN 10
    END,
    Final_Risk_Profile_ID;
