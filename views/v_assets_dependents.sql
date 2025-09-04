-- View 1.4: Relationship between Available Assets and Number of Dependents grouped and without grouping
CREATE OR REPLACE VIEW v_assets_dependents AS
SELECT c.Customer_ID, c.Number_of_Dependents, ca.Total AS Available_Assets
FROM customers c
JOIN customer_assets ca ON c.Customer_ID = ca.Customer_ID
WHERE NOT (c.Number_of_Dependents > 0 AND TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', '')) < 100000);

CREATE OR REPLACE VIEW v_assets_dependents_grouped_statistics AS
SELECT 
    CASE 
        WHEN number_of_dependents = 0 THEN 'No Dependents'
        WHEN number_of_dependents BETWEEN 1 AND 2 THEN '1-2 Dependents'
        WHEN number_of_dependents BETWEEN 3 AND 4 THEN '3-4 Dependents'
        WHEN number_of_dependents > 4 THEN '5+ Dependents'
    END AS dependent_group,
    COUNT(*) as customer_count,
    ROUND(AVG(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) as avg_assets,
    ROUND(MIN(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) as min_assets,
    ROUND(MAX(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) as max_assets,
    ROUND(STDDEV(TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', ''))), 0) as std_dev_assets
FROM customers c
JOIN customer_assets ca ON c.customer_id = ca.customer_id
WHERE NOT (c.number_of_dependents > 0 AND TRY_TO_NUMBER(REGEXP_REPLACE(ca.total, '[^0-9.-]', '')) < 100000)
GROUP BY 
    CASE 
        WHEN number_of_dependents = 0 THEN 'No Dependents'
        WHEN number_of_dependents BETWEEN 1 AND 2 THEN '1-2 Dependents'
        WHEN number_of_dependents BETWEEN 3 AND 4 THEN '3-4 Dependents'
        WHEN number_of_dependents > 4 THEN '5+ Dependents'
    END
ORDER BY dependent_group;