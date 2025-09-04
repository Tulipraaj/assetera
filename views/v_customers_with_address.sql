CREATE OR REPLACE VIEW v_customers_with_address AS
SELECT 
    c.*,
    Contact_First_Name || ' ' || Contact_Last_Name || CHR(10) ||
    Street || CHR(10) ||
    City || ', ' || State || ' ' || Zip AS Formatted_Mailing_Address
FROM Customers c;
