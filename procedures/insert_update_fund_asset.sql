CREATE OR REPLACE PROCEDURE safe_insert_fund_asset(
    p_fund_id INT,
    p_asset_class_id INT, 
    p_percent_of_fund FLOAT
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
'
var checkStmt = snowflake.createStatement({
    sqlText: "SELECT COALESCE(SUM(percent_of_fund), 0) FROM fund_assets WHERE fund_id = ?",
    binds: [p_fund_id]
});

var result = checkStmt.execute();
result.next();
var currentTotal = result.getColumnValue(1);

if (currentTotal + p_percent_of_fund > 100) {
    throw "Total allocation would exceed 100% for fund " + p_fund_id;
}

var insertStmt = snowflake.createStatement({
    sqlText: "INSERT INTO fund_assets (fund_id, asset_class_id, percent_of_fund) VALUES (?, ?, ?)",
    binds: [p_fund_id, p_asset_class_id, p_percent_of_fund]
});

insertStmt.execute();
return "Successfully inserted";
';

CREATE OR REPLACE PROCEDURE safe_update_fund_asset(
    p_fund_id INT,
    p_asset_class_id INT,
    p_new_percent FLOAT
)
RETURNS STRING  
LANGUAGE JAVASCRIPT
AS
'
var checkStmt = snowflake.createStatement({
    sqlText: "SELECT COALESCE(SUM(percent_of_fund), 0) FROM fund_assets WHERE fund_id = ? AND asset_class_id != ?",
    binds: [p_fund_id, p_asset_class_id]
});

var result = checkStmt.execute();
result.next();
var otherTotal = result.getColumnValue(1);

if (otherTotal + p_new_percent > 100) {
    throw "Total allocation would exceed 100% for fund " + p_fund_id;
}

var updateStmt = snowflake.createStatement({
    sqlText: "UPDATE fund_assets SET percent_of_fund = ? WHERE fund_id = ? AND asset_class_id = ?",
    binds: [p_new_percent, p_fund_id, p_asset_class_id]
});

updateStmt.execute();
return "Successfully updated";
';
