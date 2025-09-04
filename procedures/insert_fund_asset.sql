CREATE OR REPLACE PROCEDURE insert_fund_asset (
    p_fund_id INT,
    p_asset_class_id INT,
    p_percent_of_fund FLOAT
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_total FLOAT;
BEGIN
    -- Check current allocation for this fund
    LET v_total = COALESCE((
        SELECT SUM(percent_of_fund)
        FROM fund_assets
        WHERE fund_id = p_fund_id
    ), 0);

    -- If new allocation exceeds 100%, raise error
    IF v_total + p_percent_of_fund > 100 THEN
        RAISE STATEMENT_ERROR 
            USING MESSAGE = '❌ Allocation exceeds 100% for fund ' || p_fund_id;
    END IF;

    -- Otherwise insert
    INSERT INTO fund_assets (fund_id, asset_class_id, percent_of_fund)
    VALUES (p_fund_id, p_asset_class_id, p_percent_of_fund);

    RETURN '✅ Inserted successfully';
END;
