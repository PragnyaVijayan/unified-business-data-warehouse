/*
============================================
Create DB and Schemas
============================================
PURPOSE : 
    This script creates a new database called DataWarehouse after checking if it already exists.
    If it already exists, it is dropped and recreased. Sets up schemas for medallion architecture.

WARNING:
    Executing this script will drop the entire 'DataWarehouse' databasem, as well as the medallion
    schemas if they already exist. Ensure there are proper backups before running this script.
-- 
*/


-- Ensure we are working with the 'master' database
USE master;
GO

-- Drop and recreate the 'DataWarehouse' database --
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created DataWarehouse database
USE DataWarehouse;
GO

-- Ensure QUOTED_IDENTIFIER is ON for creating schemas with proper handling
SET QUOTED_IDENTIFIER ON;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
