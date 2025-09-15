SELECT 
    c.state,
    SUM((TO_NUMBER(REPLACE(REPLACE(ca.total, '$', ''), ',', '')))) AS Total_Assets
FROM 
    Customers c
JOIN 
    Customer_Assets ca 
    ON c.Customer_ID = ca.Customer_ID
GROUP BY 
    c.state
ORDER BY 
    Total_Assets DESC;


select * from V_AGE_GENDER_MARITAL_DEPENDENTS_REGION_ECONOMY_INCOME_RISK_ASSETS;

select * from c##t_mask."V_temp2";