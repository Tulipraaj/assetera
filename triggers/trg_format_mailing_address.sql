
CREATE OR REPLACE TRIGGER trg_format_mailing_address
BEFORE INSERT OR UPDATE ON Customers
FOR EACH ROW
BEGIN
    :NEW.Formatted_Mailing_Address :=
        :NEW.Contact_First_Name || ' ' || :NEW.Contact_Last_Name || CHR(10) ||
        :NEW.Street || CHR(10) ||
        :NEW.City || ', ' || :NEW.State || ' ' || :NEW.Zip;
END;
/
