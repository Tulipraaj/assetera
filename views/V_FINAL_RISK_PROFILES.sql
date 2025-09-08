CREATE OR REPLACE VIEW V_FINAL_RISK_PROFILES AS
SELECT 
    ca.customer_id,
    CASE 
        WHEN mode_risk IN (1,2) THEN FLOOR(avg_risk)
        WHEN mode_risk IN (4,5) THEN CEIL(avg_risk)
        ELSE ROUND(avg_risk)
    END AS final_risk
FROM (
    SELECT 
        ca.customer_id,
        AVG(rp.risk_profile_id) AS avg_risk,
        (
            SELECT rp2.risk_profile_id
            FROM customer_answers ca2
            JOIN answers a2
              ON ca2.question_id = a2.question_id
             AND ca2.answer_id = a2.answer_id
            JOIN risk_profile rp2
              ON a2.risk_profile_id = rp2.risk_profile_id
            WHERE ca2.customer_id = ca_outer.customer_id
            GROUP BY rp2.risk_profile_id
            ORDER BY COUNT(*) DESC, rp2.risk_profile_id ASC
            LIMIT 1
        ) AS mode_risk
    FROM customer_answers ca_outer
    JOIN answers a
      ON ca_outer.question_id = a.question_id
     AND ca_outer.answer_id = a.answer_id
    JOIN risk_profile rp
      ON a.risk_profile_id = rp.risk_profile_id
    GROUP BY ca_outer.customer_id
) t;
