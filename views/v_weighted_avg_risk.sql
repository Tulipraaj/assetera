--To get the weighted average risk for created funds
CREATE OR REPLACE VIEW v_weighted_avg_risk AS
SELECT 
    fa.Fund_ID,
    pf.Fund_Name,
    ROUND(SUM(fa.Percent_of_Fund * ac.Risk_Profile_ID)/100) AS Weighted_Average_Risk_Score
FROM Fund_Assets fa
JOIN Asset_Classes ac ON fa.Asset_Class_ID = ac.Asset_Class
JOIN Potential_Funds pf ON fa.Fund_ID = pf.Fund_ID
GROUP BY fa.Fund_ID, pf.Fund_Name;