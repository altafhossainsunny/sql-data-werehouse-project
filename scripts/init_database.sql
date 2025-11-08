/*=============================================================
  Project: Data Warehousing with SQL Server
  Author:  MD ALTAF HOSSAIN SUNNY
  Purpose: Create Data Warehouse and Schemas
==============================================================*/

/*===============================================================

  ⚠️  WARNING:
  --------------------------------------------------------------
  - This script will DROP the existing 'DataWarehouse' database 
    if it already exists in your SQL Server instance.
  - All data inside that database will be permanently deleted.
  - Make sure to BACKUP any important data before running this.
  --------------------------------------------------------------
==============================================================*/

--=============================================================
-- Step 1: Use master database
--=============================================================
USE master;
GO

--=============================================================
-- Step 2: Check if the DataWarehouse database already exists
--          If exists, set to SINGLE_USER and drop it safely
--=============================================================
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--=============================================================
-- Step 3: Create a new DataWarehouse database
--=============================================================
CREATE DATABASE DataWarehouse;
GO

-- Switch to the newly created database
USE DataWarehouse;
GO

--=============================================================
-- Step 4: Create separate schemas for data layers
--=============================================================
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

--=============================================================
-- ✅ Data Warehouse successfully initialized
--    Schemas created: bronze, silver, gold
--=============================================================
PRINT '✅ Data Warehouse and Schemas created successfully!';
