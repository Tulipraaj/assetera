
-- Function to compute Risk Profile of a customer given customer_id
--==================================================================
CREATE OR REPLACE FUNCTION get_final_risk_profile(p_customer_id IN NUMBER)
RETURN NUMBER
IS
    v_avg NUMBER;
    v_mode NUMBER;
    v_final NUMBER;
BEGIN
    -- Compute average and mode
    SELECT AVG(rp.risk_profile_id),
           MAX(rp.risk_profile_id) KEEP (
               DENSE_RANK FIRST ORDER BY COUNT(*) DESC
           )
    INTO v_avg, v_mode
    FROM customer_answers ca
    JOIN answers a
      ON ca.question_id = a.question_id
     AND ca.answer_id = a.answer_id
    JOIN risk_profile rp
      ON a.risk_profile_id = rp.risk_profile_id
    WHERE ca.customer_id = p_customer_id
    GROUP BY ca.customer_id;

    -- Apply adjustment rules
    IF v_mode IN (1,2) THEN
        v_final := FLOOR(v_avg);
    ELSIF v_mode IN (4,5) THEN
        v_final := CEIL(v_avg);
    ELSE
        v_final := ROUND(v_avg);
    END IF;

    RETURN v_final;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;  -- in case customer has no answers
END;
/
