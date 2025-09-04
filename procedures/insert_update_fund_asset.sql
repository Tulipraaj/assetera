CREATE OR REPLACE PROCEDURE safe_insert_fund_asset(p_fund_id FLOAT, p_asset_class_id FLOAT, p_percent_of_fund FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
'
var checkStmt = snowflake.createStatement({sqlText: "SELECT COALESCE(SUM(percent_of_fund), 0) FROM fund_assets WHERE fund_id = " + p_fund_id})
var result = checkStmt.execute()
result.next()
var currentTotal = result.getColumnValue(1)
if (currentTotal + p_percent_of_fund > 100) {
    return "ERROR: Total allocation would exceed 100%"
}
var insertStmt = snowflake.createStatement({sqlText: "INSERT INTO fund_assets VALUES (" + p_fund_id + ", " + p_asset_class_id + ", " + p_percent_of_fund + ")"})
insertStmt.execute()
return "SUCCESS: Record inserted"
';

-- Simple JavaScript procedure for updates
CREATE OR REPLACE PROCEDURE safe_update_fund_asset(p_fund_id FLOAT, p_asset_class_id FLOAT, p_new_percent FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
'
var checkStmt = snowflake.createStatement({sqlText: "SELECT COALESCE(SUM(percent_of_fund), 0) FROM fund_assets WHERE fund_id = " + p_fund_id + " AND asset_class_id != " + p_asset_class_id})
var result = checkStmt.execute()
result.next()
var otherTotal = result.getColumnValue(1)
if (otherTotal + p_new_percent > 100) {
    return "ERROR: Total allocation would exceed 100%"
}
var updateStmt = snowflake.createStatement({sqlText: "UPDATE fund_assets SET percent_of_fund = " + p_new_percent + " WHERE fund_id = " + p_fund_id + " AND asset_class_id = " + p_asset_class_id})
updateStmt.execute()
return "SUCCESS: Record updated"
';
