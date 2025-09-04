--To Ensure the percentage distribution within the fund assets table cannot exceed 100%
CREATE OR REPLACE TRIGGER trg_check_fund_allocation
BEFORE INSERT OR UPDATE ON fund_assets
FOR EACH ROW
DECLARE
    v_total NUMBER;
BEGIN
    SELECT NVL(SUM(percent_of_fund),0)
    INTO v_total
    FROM fund_assets
    WHERE fund_id = :NEW.fund_id;

    IF v_total + :NEW.percent_of_fund > 100 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Total allocation for fund exceeds 100%');
    END IF;
END;
/