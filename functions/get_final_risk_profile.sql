CREATE OR REPLACE FUNCTION get_final_risk_profile(p_customer_id INT)
RETURNS NUMBER
AS
$$
    WITH base AS (
        SELECT 
            AVG(rp.risk_profile_id) AS avg_risk,
            ARRAY_AGG(rp.risk_profile_id 
                      ORDER BY COUNT(*) DESC, rp.risk_profile_id ASC 
                      LIMIT 1)[0] AS mode_risk
        FROM customer_answers ca
        JOIN answers a
          ON ca.question_id = a.question_id
         AND ca.answer_id = a.answer_id
        JOIN risk_profile rp
          ON a.risk_profile_id = rp.risk_profile_id
        WHERE ca.customer_id = p_customer_id
        GROUP BY ca.customer_id
    )
    SELECT CASE
               WHEN mode_risk IN (1,2) THEN FLOOR(avg_risk)
               WHEN mode_risk IN (4,5) THEN CEIL(avg_risk)
               ELSE ROUND(avg_risk)
           END
    FROM base
$$;
