CREATE OR REPLACE VIEW V_FINAL_RISK_PROFILES_WINDOW AS
WITH customer_risk_data AS (
    SELECT 
        ca.customer_id,
        rp.risk_profile_id,
        COUNT(*) as answer_count,
        ROW_NUMBER() OVER (
            PARTITION BY ca.customer_id 
            ORDER BY COUNT(*) DESC, rp.risk_profile_id ASC
        ) as rn
    FROM customer_answers ca
    JOIN answers a
      ON ca.question_id = a.question_id
     AND ca.answer_id = a.answer_id
    JOIN risk_profile rp
      ON a.risk_profile_id = rp.risk_profile_id
    GROUP BY ca.customer_id, rp.risk_profile_id
),
customer_stats AS (
    SELECT 
        ca.customer_id,
        AVG(rp.risk_profile_id::FLOAT) AS avg_risk
    FROM customer_answers ca
    JOIN answers a
      ON ca.question_id = a.question_id
     AND ca.answer_id = a.answer_id
    JOIN risk_profile rp
      ON a.risk_profile_id = rp.risk_profile_id
    GROUP BY ca.customer_id
)
SELECT 
    cs.customer_id,
    CASE 
        WHEN crd.risk_profile_id IN (1,2) THEN FLOOR(cs.avg_risk)
        WHEN crd.risk_profile_id IN (4,5) THEN CEIL(cs.avg_risk)
        ELSE ROUND(cs.avg_risk)
    END AS final_risk
FROM customer_stats cs
JOIN customer_risk_data crd
  ON cs.customer_id = crd.customer_id
 AND crd.rn = 1;
