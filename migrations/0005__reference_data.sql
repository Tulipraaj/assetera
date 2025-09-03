CREATE OR REPLACE TABLE currencies (
    code STRING PRIMARY KEY,
    name STRING
);

INSERT INTO currencies (code, name)
VALUES 
  ('USD', 'US Dollar'),
  ('EUR', 'Euro'),
  ('INR', 'Indian Rupee');
