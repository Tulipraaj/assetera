CREATE OR REPLACE FUNCTION fn_weighted_risk(fund_id INT)
RETURNS NUMBER
AS
$$
    SELECT ROUND(SUM((fa.percent_of_fund/100) * ac.risk_profile_id))
    FROM fund_assets fa
    JOIN asset_classes ac 
        ON fa.asset_class_id = ac.asset_class
    WHERE fa.fund_id = fund_id
$$;
