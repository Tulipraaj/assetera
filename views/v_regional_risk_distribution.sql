CREATE OR REPLACE VIEW v_regional_risk_distribution AS
WITH customer_answer_risks AS (
    SELECT
        c.Customer_ID,
        c.state,
        c.country,
        a.Risk_Profile_ID
    FROM customers c
    JOIN customer_answers ca ON c.Customer_ID = ca.Customer_ID
    JOIN answers a ON ca.Question_ID = a.Question_ID AND ca.Answer_ID = a.Answer_ID
),
customer_mode_stats AS (
    SELECT 
        Customer_ID,
        state,
        country,
        AVG(Risk_Profile_ID) AS Avg_Risk_Profile,
        MODE() WITHIN GROUP (ORDER BY Risk_Profile_ID) AS Mode_Risk_Profile
    FROM customer_answer_risks
    GROUP BY Customer_ID, state, country
),
customer_final AS (
    SELECT
        Customer_ID,
        state,
        country,
        CASE
            WHEN Mode_Risk_Profile IN (1,2) THEN FLOOR(Avg_Risk_Profile)
            WHEN Mode_Risk_Profile IN (4,5) THEN CEIL(Avg_Risk_Profile)
            ELSE ROUND(Avg_Risk_Profile)
        END AS Final_Risk_Profile_ID
    FROM customer_mode_stats
),
regional_risk_counts AS (
    SELECT
        state,
        country,
        Final_Risk_Profile_ID,
        COUNT(*) AS customer_count
    FROM customer_final
    GROUP BY state, country, Final_Risk_Profile_ID
),
regional_totals AS (
    SELECT
        state,
        country,
        SUM(customer_count) AS total_region_customers
    FROM regional_risk_counts
    GROUP BY state, country
),
risk_totals AS (
    SELECT
        Final_Risk_Profile_ID,
        SUM(customer_count) AS total_risk_customers
    FROM regional_risk_counts
    GROUP BY Final_Risk_Profile_ID
)
SELECT
    rrc.state,
    rrc.country,
    rp.Risk_Profile,
    rrc.customer_count,
    ROUND(rrc.customer_count * 100.0 / rt.total_region_customers, 2) AS pct_within_region,
    ROUND(rrc.customer_count * 100.0 / rkt.total_risk_customers, 2) AS pct_within_risk_category
FROM regional_risk_counts rrc
JOIN regional_totals rt ON rrc.state = rt.state AND rrc.country = rt.country
JOIN risk_totals rkt ON rrc.Final_Risk_Profile_ID = rkt.Final_Risk_Profile_ID
JOIN risk_profile rp ON rrc.Final_Risk_Profile_ID = rp.Risk_Profile_ID
ORDER BY rrc.state, rrc.country, rp.Risk_Profile;
