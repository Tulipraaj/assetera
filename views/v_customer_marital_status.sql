-- View 1.3: Marital Status Distribution
CREATE OR REPLACE VIEW v_customer_marital_status AS
SELECT 
    COALESCE(Marital_Status, 'Unknown') AS Marital_Status,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(age), 1) as avg_age
FROM customers c
GROUP BY marital_status
ORDER BY customer_count DESC;