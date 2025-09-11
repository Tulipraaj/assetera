CREATE OR REPLACE FORCE EDITIONABLE VIEW C##T_MASK.V_CUSTOMER_REGION_ECONOMY AS
WITH state_region AS (
    SELECT DISTINCT
        c.state,
        CASE
            WHEN c.state IN ('Maine','Massachusetts','Connecticut','Delaware','Pennsylvania','Maryland') THEN 'Northeast'
            WHEN c.state IN ('Ohio','Illinois','Michigan','Indiana','Wisconsin','Minnesota','Iowa','Kansas','Missouri','Nebraska') THEN 'Midwest'
            WHEN c.state IN ('Florida','Kentucky','Texas','Alabama','Virginia','Georgia','Tennessee','Louisiana','Arkansas','Mississippi') THEN 'South'
            WHEN c.state IN ('California','Washington','Oregon','Nevada','Arizona','Colorado','Montana','Wyoming','Idaho','Hawaii','Alaska') THEN 'West'
            ELSE 'Other'
        END AS region
    FROM c##t_mask.customers c
),
state_economy AS (
    SELECT DISTINCT
        c.state,
        CASE
            WHEN c.state IN ('California','Washington','Massachusetts','Connecticut','Alaska','Maryland') THEN 'High Income'
            WHEN c.state IN ('Virginia','Colorado','Hawaii','Minnesota','Oregon') THEN 'Upper-Mid Income'
            WHEN c.state IN ('Texas','Illinois','Pennsylvania','Wisconsin','Nevada','Delaware') THEN 'Middle Income'
            WHEN c.state IN ('Florida','Ohio','Michigan','Indiana','Arizona','Georgia') THEN 'Lower-Mid Income'
            WHEN c.state IN ('Mississippi','Arkansas','Louisiana','Kentucky','Alabama') THEN 'Low Income'
            ELSE 'Other'
        END AS economic_class
    FROM c##t_mask.customers c
)
SELECT 
    ag.age_group,
    ag.gender,
    ag.marital_status,
    ag.dependents,
    rp.final_risk_profile,
    r.region,
    e.economic_class,
    COUNT(DISTINCT ag.customer_id) AS customer_count,
    ROUND(SUM(NVL(ca.total_assets,0)), 0) AS total_assets,
    ROUND(AVG(NVL(ca.total_assets,0)), 0) AS avg_assets,
    ROUND(MIN(NVL(ca.total_assets,0)), 0) AS min_assets,
    ROUND(MAX(NVL(ca.total_assets,0)), 0) AS max_assets,
    ROUND(STDDEV(NVL(ca.total_assets,0)), 0) AS std_dev_assets
FROM age_gender_marital_dependents_groups ag
JOIN risk_profiles rp ON ag.customer_id = rp.customer_id
LEFT JOIN cleaned_assets ca ON ag.customer_id = ca.customer_id
LEFT JOIN state_region r ON ag.state = r.state
LEFT JOIN state_economy e ON ag.state = e.state
GROUP BY ag.age_group, ag.gender, ag.marital_status, ag.dependents, rp.final_risk_profile, r.region, e.economic_class
ORDER BY ag.age_group, ag.gender, ag.marital_status, ag.dependents, rp.final_risk_profile, r.region, e.economic_class;
