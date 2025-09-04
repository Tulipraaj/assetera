-- View 1.2: Regional Distribution of Investment Customers
CREATE OR REPLACE VIEW v_customer_regional_distribution AS
SELECT 
    state,
    country,
    COUNT(*) as customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM customers c
GROUP BY state, country  
ORDER BY customer_count DESC;