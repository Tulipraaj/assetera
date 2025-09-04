CREATE OR REPLACE PROCEDURE insert_fund_asset (
    p_fund_id INT,
    p_asset_class_id INT,
    p_percent_of_fund FLOAT
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
'
    var stmt1 = snowflake.createStatement({
        sqlText: "SELECT COALESCE(SUM(percent_of_fund), 0) as total FROM fund_assets WHERE fund_id = ?",
        binds: [p_fund_id]
    });
    
    var result1 = stmt1.execute();
    result1.next();
    var currentTotal = result1.getColumnValue(1);
    
    if (currentTotal + p_percent_of_fund > 100) {
        return "Allocation exceeds 100% for fund " + p_fund_id;
    }
    
    var stmt2 = snowflake.createStatement({
        sqlText: "INSERT INTO fund_assets (fund_id, asset_class_id, percent_of_fund) VALUES (?, ?, ?)",
        binds: [p_fund_id, p_asset_class_id, p_percent_of_fund]
    });
    
    stmt2.execute();
    
    return "Inserted successfully";
';
