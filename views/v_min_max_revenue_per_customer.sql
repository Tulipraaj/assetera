--Minimum and maximum revenue per customer for created funds
CREATE OR REPLACE VIEW v_min_max_revenue_per_customer AS
SELECT
    pf.Fund_ID,
    pf.Fund_Name,
    SUM(
        fs.Flat_Fee + (fs.Percentage_Fee / 100.0) * pf.Minimum_Investment_Required * fa.Percent_Of_Fund / 100.0
    ) AS Min_Revenue_Per_Customer,
    SUM(
        fs.Flat_Fee + (fs.Percentage_Fee / 100.0) * pf.Maximum_Investment_Allowed * fa.Percent_Of_Fund / 100.0
    ) AS Max_Revenue_Per_Customer
FROM Potential_Funds pf
JOIN Fund_Assets fa ON pf.Fund_ID = fa.Fund_ID
JOIN Fee_Structure fs ON fa.Asset_Class_ID = fs.Asset_Class_ID
GROUP BY pf.Fund_ID, pf.Fund_Name
ORDER BY pf.Fund_ID;
