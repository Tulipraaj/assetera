-- Relation between customer risk profile & assets they possess
CREATE OR REPLACE VIEW v_risk_category_assets AS
WITH customer_answer_risks AS (
    SELECT
        c.Customer_ID,
        a.Risk_Profile_ID
    FROM customers c
    JOIN customer_answers ca ON c.Customer_ID = ca.Customer_ID
    JOIN answers a ON ca.Question_ID = a.Question_ID AND ca.Answer_ID = a.Answer_ID
),
customer_avg_stats AS (
    SELECT
        Customer_ID,
        AVG(Risk_Profile_ID) AS Avg_Risk_Profile
    FROM customer_answer_risks
    GROUP BY Customer_ID
),
customer_mode_stats AS (
    SELECT
        Customer_ID,
        Risk_Profile_ID AS Mode_Risk_Profile
    FROM (
        SELECT
            Customer_ID,
            Risk_Profile_ID,
            COUNT(*) AS freq,
            ROW_NUMBER() OVER (
                PARTITION BY Customer_ID
                ORDER BY COUNT(*) DESC, Risk_Profile_ID
            ) AS rn
        FROM customer_answer_risks
        GROUP BY Customer_ID, Risk_Profile_ID
    ) t
    WHERE rn = 1
),
customer_stats AS (
    SELECT
        avg_stats.Customer_ID,
        avg_stats.Avg_Risk_Profile,
        mode_stats.Mode_Risk_Profile
    FROM customer_avg_stats avg_stats
    JOIN customer_mode_stats mode_stats
        ON avg_stats.Customer_ID = mode_stats.Customer_ID
),
customer_final AS (
    SELECT
        Customer_ID,
        CASE
            WHEN Mode_Risk_Profile IN (1,2) THEN FLOOR(Avg_Risk_Profile)
            WHEN Mode_Risk_Profile IN (4,5) THEN CEIL(Avg_Risk_Profile)
            ELSE ROUND(Avg_Risk_Profile)
        END AS Final_Risk_Profile_ID
    FROM customer_stats
),
customer_assets_parsed AS (
    SELECT
        Customer_ID,
        TRY_TO_NUMBER(REGEXP_REPLACE(total, '[^0-9.-]', '')) AS total_numeric
    FROM customer_assets
)
SELECT 
    cf.Final_Risk_Profile_ID,
    rp.Risk_Profile,
    COUNT(DISTINCT cf.Customer_ID) AS Customer_Count,
    ROUND(AVG(cas.total_numeric), 0) AS Avg_Assets,
    ROUND(MIN(cas.total_numeric), 0) AS Min_Assets,
    ROUND(MAX(cas.total_numeric), 0) AS Max_Assets,
    ROUND(MEDIAN(cas.total_numeric), 0) AS Median_Assets,
    ROUND(STDDEV(cas.total_numeric), 0) AS Std_Dev_Assets
FROM customer_final cf
JOIN risk_profile rp ON cf.Final_Risk_Profile_ID = rp.Risk_Profile_ID
JOIN customer_assets_parsed cas ON cf.Customer_ID = cas.Customer_ID
GROUP BY cf.Final_Risk_Profile_ID, rp.Risk_Profile
ORDER BY cf.Final_Risk_Profile_ID;

