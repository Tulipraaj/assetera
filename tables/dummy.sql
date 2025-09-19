-- Print which DB we are connected to
SELECT CURRENT_DATABASE() AS ACTIVE_DATABASE;

-- Create a dummy table to confirm deployment
CREATE OR REPLACE TABLE deploy_test (
    id INT,
    note STRING
);

-- Insert a row mentioning branch + timestamp
INSERT INTO deploy_test
SELECT 1, CONCAT('Deployed on ', CURRENT_DATABASE(), ' at ', CURRENT_TIMESTAMP());

-- Show the inserted row
SELECT * FROM deploy_test;
