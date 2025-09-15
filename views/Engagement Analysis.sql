-- convert this oracle script into snowflake sql

--1. Customer Preferred Contact Method
CREATE OR REPLACE VIEW v_customer_pref_contact AS
SELECT 
    c.Customer_ID,
    c.Contact_First_Name,
    et.Engagement_Type_Name AS Preferred_Channel,
    ef.Frequency_Name AS Preferred_Frequency
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID;

--2. Engagement Type Vs Customer Count
CREATE OR REPLACE VIEW v_eng_type_customer_count AS
SELECT et.Engagement_Type_Name, COUNT(*) AS num_customers
FROM Customer_Engagement_Preferences cep
JOIN Engagement_Types et ON cep.Engagement_Type_ID = et.Engagement_Type_ID
GROUP BY et.Engagement_Type_Name
ORDER BY num_customers DESC;

--3. Engagement Frequency Vs Customer Count
CREATE OR REPLACE VIEW v_eng_freq_customer_count AS
SELECT ef.Frequency_Name, COUNT(*) AS num_customers
FROM Customer_Engagement_Preferences cep
JOIN Engagement_Frequencies ef ON cep.Frequency_ID = ef.Frequency_ID
GROUP BY ef.Frequency_Name
ORDER BY num_customers DESC;

--4. Customer count grouped by engagement type & engagement frequency
CREATE OR REPLACE VIEW v_eng_type_freq_cus_count AS
SELECT et.Engagement_Type_Name, ef.Frequency_Name, COUNT(*) AS num_customers
FROM Customer_Engagement_Preferences cep
JOIN Engagement_Types et ON cep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON cep.Frequency_ID = ef.Frequency_ID
GROUP BY et.Engagement_Type_Name, ef.Frequency_Name
ORDER BY et.Engagement_Type_Name, num_customers DESC;

--5. To get customer risk profile & preferred contact method
CREATE OR REPLACE VIEW v_customer_risk_pref_contact AS
SELECT 
    c.Customer_ID,
    c.Contact_First_Name,
    et.Engagement_Type_Name AS Preferred_Channel,
    ef.Frequency_Name AS Preferred_Frequency,
    get_final_risk_profile(c.Customer_ID) AS Customer_Risk_Profile
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID;

--6. Customer risk profile & preferred contact method, customer count
CREATE OR REPLACE VIEW v_risk_eng_type_freq AS
SELECT
    get_final_risk_profile(c.Customer_ID) AS Customer_Risk_Profile,
    et.Engagement_Type_Name AS Preferred_Channel,
    ef.Frequency_Name AS Preferred_Frequency,
    COUNT(c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID
GROUP BY et.Engagement_Type_Name, ef.Frequency_Name, get_final_risk_profile(c.Customer_ID)
ORDER BY Customer_Risk_Profile, Preferred_Channel, Preferred_Frequency;

--7. Customer risk profile vs engagement frequency, customer count
CREATE OR REPLACE VIEW v_risk_eng_freq AS
SELECT
    get_final_risk_profile(c.Customer_ID) AS Customer_Risk_Profile,
    ef.Frequency_Name AS Preferred_Frequency,
    COUNT(c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID
GROUP BY ef.Frequency_Name, get_final_risk_profile(c.Customer_ID)

ORDER BY Customer_Risk_Profile, Preferred_Frequency;

--
CREATE OR REPLACE VIEW v_eng_type_location_count AS
SELECT
    et.Engagement_Type_Name AS Engagement_Type,
    c.State,
    COUNT(DISTINCT c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
GROUP BY c.State, et.Engagement_Type_Name
ORDER BY c.State, et.Engagement_Type_Name;

-------
CREATE OR REPLACE VIEW v_eng_type_location_freq_count AS
SELECT
    et.Engagement_Type_Name AS Engagement_Type,
    ef.Frequency_Name AS Frequency,
    c.State,
    COUNT(DISTINCT c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID
GROUP BY c.State, et.Engagement_Type_Name, ef.Frequency_Name
ORDER BY c.State, et.Engagement_Type_Name, ef.Frequency_Name;

--

CREATE OR REPLACE VIEW v_eng_freq_location_count AS
SELECT
    ef.Frequency_Name AS Frequency,
    c.State,
    COUNT(DISTINCT c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID
GROUP BY c.State, ef.Frequency_Name
ORDER BY c.State, ef.Frequency_Name;

--

CREATE OR REPLACE VIEW v_agegroup_eng_type_count AS
SELECT
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
         WHEN age BETWEEN 60 AND 64 THEN '60-64'
         WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
    END AS age_group,
    et.Engagement_Type_Name AS Engagement_Type,
    COUNT(DISTINCT c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
GROUP BY 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
         WHEN age BETWEEN 60 AND 64 THEN '60-64'
         WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
        END,
    et.Engagement_Type_Name
ORDER BY Age_Group, et.Engagement_Type_Name;

--
CREATE OR REPLACE VIEW v_agegroup_eng_type_freq_count AS
SELECT
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
        WHEN age BETWEEN 60 AND 64 THEN '60-64'
        WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
    END AS age_group,
    et.Engagement_Type_Name AS Engagement_Type,
    ef.Frequency_Name AS Frequency,
    COUNT(DISTINCT c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
JOIN Engagement_Frequencies ef ON ep.Frequency_ID = ef.Frequency_ID
GROUP BY 
    CASE 
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 34 THEN '30-34'
        WHEN age BETWEEN 35 AND 39 THEN '35-39'
        WHEN age BETWEEN 40 AND 44 THEN '40-44'
        WHEN age BETWEEN 45 AND 49 THEN '45-49'
        WHEN age BETWEEN 50 AND 54 THEN '50-54'
        WHEN age BETWEEN 55 AND 59 THEN '55-59'
        WHEN age BETWEEN 60 AND 64 THEN '60-64'
        WHEN age BETWEEN 65 AND 69 THEN '65-69'
        WHEN age >= 70 THEN 'Above 70'
        ELSE 'Unknown'
    END,
    et.Engagement_Type_Name,
    ef.Frequency_Name
ORDER BY age_group, et.Engagement_Type_Name, ef.Frequency_Name;

--
CREATE OR REPLACE VIEW v_risk_eng_type AS
SELECT
    get_final_risk_profile(c.Customer_ID) as Customer_Risk_Profile,
    et.Engagement_Type_Name AS Preferred_Channel,
    count(c.Customer_ID) AS customer_count
FROM Customers c
JOIN Customer_Engagement_Preferences ep ON c.Customer_ID = ep.Customer_ID
JOIN Engagement_Types et ON ep.Engagement_Type_ID = et.Engagement_Type_ID
GROUP BY et.Engagement_Type_Name, get_final_risk_profile(c.Customer_ID)
ORDER BY Customer_Risk_Profile, Preferred_Channel;
