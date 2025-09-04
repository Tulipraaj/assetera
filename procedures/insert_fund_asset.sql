CREATE OR REPLACE PROCEDURE insert_fund_asset (
    p_fund_id INT,
    p_asset_class_id INT,
    p_percent_of_fund FLOAT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_total FLOAT;
    allocation_violation EXCEPTION;
BEGIN
    -- Check current allocation for this fund
    SELECT COALESCE(SUM(percent_of_fund),0)
    INTO v_total
    FROM fund_assets
    WHERE fund_id = p_fund_id;

    -- If new allocation exceeds 100%, raise error
    IF v_total + p_percent_of_fund > 100 THEN
        RAISE allocation_violation 
          USING MESSAGE = '❌ Allocation exceeds 100% for fund ' || p_fund_id;
    END IF;

    -- Otherwise insert
    INSERT INTO fund_assets (fund_id, asset_class_id, percent_of_fund)
    VALUES (p_fund_id, p_asset_class_id, p_percent_of_fund);

    RETURN '✅ Inserted successfully';
END;
$$;
