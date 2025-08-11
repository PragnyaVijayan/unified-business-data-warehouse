/*
============================================
Create DB and Schemas
============================================
PURPOSE : 
    This script creates a new database called DataWarehouse after checking if it already exists.
    If it already exists, it is dropped and recreated. Sets up schemas for medallion architecture.

WARNING:
    Executing this script will drop the entire 'DataWarehouse' database, as well as the medallion
    schemas if they already exist. Ensure there are proper backups before running this script.
-- 
*/

-- Note: In PostgreSQL, you cannot drop/create a database within a transaction.
-- These commands need to be run separately or from psql.

-- Drop and recreate the 'DataWarehouse' database
-- Note: Need to disconnect all users first
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'DataWarehouse' 
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "DataWarehouse";

CREATE DATABASE "DataWarehouse";

-- Connect to the new database
\c DataWarehouse

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Set search path (optional, but helpful)
SET search_path TO bronze, silver, gold, public;
