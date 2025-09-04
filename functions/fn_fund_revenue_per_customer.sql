
CREATE OR REPLACE FUNCTION fn_fund_revenue_per_customer (p_Fund_ID INT)
RETURN fund_revenue_table PIPELINED
AS
BEGIN
    FOR rec IN (
        SELECT
            pf.Fund_ID,
            pf.Fund_Name,
            SUM(
                fs.Flat_Fee + 
                (fs.Percentage_Fee / 100.0) * pf.Minimum_Investment_Required * fa.Percent_Of_Fund / 100.0
            ) AS Min_Revenue_Per_Customer,
            SUM(
                fs.Flat_Fee + 
                (fs.Percentage_Fee / 100.0) * pf.Maximum_Investment_Allowed * fa.Percent_Of_Fund / 100.0
            ) AS Max_Revenue_Per_Customer
        FROM Potential_Funds pf
        JOIN Fund_Assets fa ON pf.Fund_ID = fa.Fund_ID
        JOIN Fee_Structure fs ON fa.Asset_Class_ID = fs.Asset_Class_ID
        WHERE pf.Fund_ID = p_Fund_ID
        GROUP BY pf.Fund_ID, pf.Fund_Name
    ) LOOP
        PIPE ROW (
            fund_revenue_record(
                rec.Fund_ID,
                rec.Fund_Name,
                rec.Min_Revenue_Per_Customer,
                rec.Max_Revenue_Per_Customer
            )
        );
    END LOOP;
    RETURN;
END;
/
