--Calculating the weighted average risk score of your fund samples
CREATE OR REPLACE FUNCTION fn_weighted_risk (fund_id IN NUMBER)
RETURN NUMBER
IS
    v_weighted_score NUMBER;
BEGIN
    SELECT ROUND(SUM((fa.percent_of_fund/100) * ac.risk_profile_id))
    INTO v_weighted_score
    FROM fund_assets fa
    JOIN asset_classes ac ON fa.asset_class_id = ac.asset_class
    WHERE fa.fund_id = fund_id;

    RETURN v_weighted_score;
END;
/