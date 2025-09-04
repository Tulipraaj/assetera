CREATE OR REPLACE TYPE fund_revenue_record AS OBJECT (
    Fund_ID INT,
    Fund_Name VARCHAR2(100),
    Min_Revenue_Per_Customer NUMBER,
    Max_Revenue_Per_Customer NUMBER
);
/

CREATE OR REPLACE TYPE fund_revenue_table AS TABLE OF fund_revenue_record;
/