-- SET previous_role = CURRENT_ROLE();
-- SET previous_database = CURRENT_DATABASE();


USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE test_db;

-- Assume Snowflake ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create a new role 'test_role'
CREATE OR REPLACE ROLE test_role;

-- Grant some privileges to 'test_role'
GRANT USAGE ON DATABASE test_db TO ROLE test_role;
GRANT USAGE ON SCHEMA test_db.public TO ROLE test_role;


-- Create a file format for CSV files
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

/*
USE ROLE IDENTIFIER($previous_role);
USE DATABASE IDENTIFIER($previous_database);
*/