-- Working JavaScript function that handles NUMBER(38,0) customer_id
CREATE OR REPLACE FUNCTION get_final_risk_profile(p_customer_id VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
'
var customer_id = p_customer_id;

var stmt1 = snowflake.createStatement({
    sqlText: "SELECT AVG(rp.risk_profile_id::FLOAT) as avg_risk FROM customer_answers ca JOIN answers a ON ca.question_id = a.question_id AND ca.answer_id = a.answer_id JOIN risk_profile rp ON a.risk_profile_id = rp.risk_profile_id WHERE ca.customer_id = ?",
    binds: [customer_id]
});

var stmt2 = snowflake.createStatement({
    sqlText: "SELECT rp.risk_profile_id FROM customer_answers ca JOIN answers a ON ca.question_id = a.question_id AND ca.answer_id = a.answer_id JOIN risk_profile rp ON a.risk_profile_id = rp.risk_profile_id WHERE ca.customer_id = ? GROUP BY rp.risk_profile_id ORDER BY COUNT(*) DESC, rp.risk_profile_id ASC LIMIT 1",
    binds: [customer_id]
});

try {
    var result1 = stmt1.execute();
    if (!result1.next()) {
        return null;
    }
    var avg_risk = result1.getColumnValue(1);
    
    var result2 = stmt2.execute();
    if (!result2.next()) {
        return null;
    }
    var mode_risk = result2.getColumnValue(1);
    
    var final_risk;
    if (mode_risk == 1 || mode_risk == 2) {
        final_risk = Math.floor(avg_risk);
    } else if (mode_risk == 4 || mode_risk == 5) {
        final_risk = Math.ceil(avg_risk);
    } else {
        final_risk = Math.round(avg_risk);
    }
    
    return final_risk;
    
} catch (err) {
    return null;
}
';
