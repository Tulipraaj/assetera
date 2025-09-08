CREATE OR REPLACE FUNCTION get_final_risk_profile_js(p_customer_id NUMBER)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
'
var stmt1 = snowflake.createStatement({
    sqlText: "SELECT AVG(rp.risk_profile_id) as avg_risk FROM customer_answers ca JOIN answers a ON ca.question_id = a.question_id AND ca.answer_id = a.answer_id JOIN risk_profile rp ON a.risk_profile_id = rp.risk_profile_id WHERE ca.customer_id = " + p_customer_id
})

var stmt2 = snowflake.createStatement({
    sqlText: "SELECT rp.risk_profile_id FROM customer_answers ca JOIN answers a ON ca.question_id = a.question_id AND ca.answer_id = a.answer_id JOIN risk_profile rp ON a.risk_profile_id = rp.risk_profile_id WHERE ca.customer_id = " + p_customer_id + " GROUP BY rp.risk_profile_id ORDER BY COUNT(*) DESC, rp.risk_profile_id ASC LIMIT 1"
})

var result1 = stmt1.execute()
var result2 = stmt2.execute()

if (!result1.next() || !result2.next()) {
    return null
}

var avg_risk = result1.getColumnValue(1)
var mode_risk = result2.getColumnValue(1)

if (mode_risk == 1 || mode_risk == 2) {
    return Math.floor(avg_risk)
} else if (mode_risk == 4 || mode_risk == 5) {
    return Math.ceil(avg_risk)
} else {
    return Math.round(avg_risk)
}
';
