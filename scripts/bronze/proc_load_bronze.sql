****************************************************************************************************
-- 📦 Stored Procedure: bronze.load_bronze
-- 🔧 Purpose:
--      This stored procedure automates the **data ingestion and staging** process for the 
--      **Bronze Layer** in the Data Warehouse using Microsoft SQL Server.
--
-- 🧭 Overview:
--      The Bronze Layer acts as the **raw data storage zone** that captures unprocessed 
--      data directly from source systems (CRM and ERP). 
--      This script ingests multiple CSV files, performs data validation, and 
--      temporarily converts data types where necessary to handle format inconsistencies.
--
-- 🧱 Data Sources:
--      1️⃣ CRM Data Sources (Customer, Product, Sales Details)
--          - cust_info.csv
--          - prd_info.csv
--          - sales_details.csv
--      2️⃣ ERP Data Sources
--          - CUST_AZ12.csv
--          - LOC_A101.csv
--          - PX_CAT_G1V2.csv
--
-- ⚙️ Key Operations:
--      • Truncate target tables before each load (Full Load pattern)
--      • Perform BULK INSERT operations from local file paths
--      • Handle mixed date formats (e.g., YYYY-MM-DD, DD/MM/YYYY)
--      • Apply TRY_CONVERT for safe type casting
--      • Log process duration and provide console feedback via PRINT statements
--      • Capture and report errors in TRY...CATCH block for robustness
--
-- 🧾 Logging Information:
--      Each section prints:
--          - Start and end notifications
--          - Load duration in seconds
--          - Sample data preview (TOP 10/100)
--          - Row count validation
--
-- 🧠 Author:   MD ALTAF HOSSAIN SUNNY
-- 📅 Created:  2025-11-11
-- 🏗️ Layer:    Bronze (Raw / Staging)
-- 🧩 Schema:   bronze
-- 💾 Database: data_warehouse_project
--
-- 🪶 Notes:
--      - Ensure file paths in BULK INSERT commands exist and have proper permissions.
--      - This procedure assumes consistent CSV structure as per the table definitions.
--      - Run with elevated privileges (BULK ADMIN or sysadmin role).
****************************************************************************************************/


CREATE OR ALTER PROCEDURE bronze.load_bronze as
  BEGIN
  Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	  BEGIN Try
		  set @batch_start_time=getdate();
		  print'=================================================================';
		  Print'Loading the bronze layer';
		  print'=================================================================';
		--========================================================================
		-- Data Ingestion and ETL Script for Bronze Schema (SQL Server)
		--========================================================================

		--===============================================================================
		-- 1. Ingesting CRM Customer Info (crm_cust_info)
		--===============================================================================
		print'=================================================================';
		Print'Loading from crm section';
		print'=================================================================';

		print'Starting to ingest the data into cust_info table';

		set @start_time=getdate();

		Truncate table bronze.crm_cust_info;


		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		set @end_time=GETDATE();

		print'Ending to ingest the data into cust_info table';
		
		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';

		--==================================================================
		-- Checking the quality of the table after insertion of crm_cust_info
		--===================================================================
		Select top 100 *
		From bronze.crm_cust_info;
		Select count(*) From bronze.crm_cust_info;


		--===============================================================================
		-- 1. Ingesting CRM product Info (crm_prd_info)
		--===============================================================================

		print'Starting to ingest the data into prd_info table';

		set @start_time=getdate();

		Truncate table bronze.crm_prd_info;


		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		set @end_time=GETDATE();
		
		print'Ending to ingest the data into prd_info table';

		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';

		--==================================================================
		-- Checking the quality of the table after insertion of crm_prd_info
		--===================================================================
		Select top 100 *
		From bronze.crm_prd_info;
		Select count(*) From bronze.crm_prd_info;

		--===================================================================================
		-- 2. Ingesting CRM Sales Details (crm_sales_details)
		-- This table requires temporary VARCHAR columns for bulk loading
		--===================================================================================;

		--===================================================================================
		-- 2. Ingesting CRM Sales Details (crm_sales_details)
		-- This table requires temporary VARCHAR columns for bulk loading and robust conversion.
		--===================================================================================
		print'Starting to ingest the data into sales_detail table';
		set @start_time=getdate();

		Truncate table bronze.crm_sales_details;

		-- Step 1: Alter data type to VARCHAR temporarily to allow BULK INSERT of potentially mixed date formats
		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_order_dt VARCHAR(20);

		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_ship_dt VARCHAR(20);

		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_due_dt VARCHAR(20);


		-- Step 2: Bulk load the data
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);

		--==================================================================================
		-- Conversion Step (CRITICAL): Convert VARCHAR columns back to DATE type
		-- This handles mixed formats (YYYY-MM-DD and DD/MM/YYYY) and converts invalid strings to NULL.
		--===================================================================================

		-- Step 3: Safely update all three columns. TRY_CONVERT returns NULL on failure.
		-- - sls_order_dt/sls_ship_dt use default style (YYYY-MM-DD).
		-- - sls_due_dt uses COALESCE to try default (ISO) first, then style 103 (DD/MM/YYYY).
		UPDATE bronze.crm_sales_details
		SET 
			sls_order_dt = TRY_CONVERT(DATE, sls_order_dt),
			sls_ship_dt = TRY_CONVERT(DATE, sls_ship_dt),
			sls_due_dt = COALESCE(
				TRY_CONVERT(DATE, sls_due_dt),        -- Try default ISO (YYYY-MM-DD)
				TRY_CONVERT(DATE, sls_due_dt, 103)    -- If that fails, try DD/MM/YYYY
			);
			set @end_time=GETDATE();
		

		-- Step 4: Change the column data type back to DATE. This succeeds because all values are now valid DATE representations or NULL.
		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_order_dt DATE;

		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_ship_dt DATE;

		ALTER TABLE bronze.crm_sales_details
		ALTER COLUMN sls_due_dt DATE;

		print'Ending to ingest the data into sales_detail table'

		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';

		--=======================================================================
		-- Health Check: Checking the quality and record count of the table
		--=======================================================================
		Select top 10 *
		From bronze.crm_sales_details;
		Select count(*) From bronze.crm_sales_details;



		--================================================================================
		-- 3. Ingesting ERP Datasets inside the bronze schema
		--================================================================================
		print'=================================================================';
		Print'Loading from erp section';
		print'=================================================================';

		print'Starting to ingest the data into erp_cust_az12 table'

		-- Now inserting the data into erp_cust_az12 table from the relavent file

		set @start_time=getdate();

		Truncate table bronze.erp_cust_az12;


		Bulk insert bronze.erp_cust_az12
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		print'Ending to ingest the data into erp_cust_az12 table';

		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';

		--=======================================================================
		-- Checking the quality of the table after insertion of erp_cust_az12
		--=======================================================================
		Select top 10 *
		From bronze.erp_cust_az12;
		Select count(*) From bronze.erp_cust_az12;


		--=========================================================================
		-- Now inserting the data into erp_loc_a101 table from the relavent file
		--===========================================================================
		print'Starting to ingest the data into erp_loc_a101 table';

		set @start_time=getdate();

		Truncate table bronze.erp_loc_a101;


		Bulk insert bronze.erp_loc_a101
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		print'Ending to ingest the data into erp_loc_a101 table';

		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';

		--=======================================================================
		-- Checking the quality of the table after insertion of erp_loc_a101
		--=======================================================================
		Select top 10 *
		From bronze.erp_loc_a101;
		Select count(*) From bronze.erp_loc_a101;

		--=======================================================================
		-- Now inserting the data into erp_loc_a101 table from the relavent file
		--=======================================================================
		print'Starting to ingest the data into erp_px_cat_g1v2 table';

		set @start_time=getdate();

		Truncate table bronze.erp_px_cat_g1v2;

		Bulk insert bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MD ALTAF HOSSAIN\Downloads\data_warehouse_project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		print'Ending to ingest the data into erp_px_cat_g1v2 table';

		print'>>Load duration: ' +cast(datediff(second ,@end_time,@start_time) as NVARCHAR) +' Seconds<<';
		print'----------------------------------------------------------------------------------------------';
		--=======================================================================
		-- Checking the quality of the table after insertion of erp_px_cat_g1v2
		--=======================================================================
		Select top 10 *
		From 
			bronze.erp_px_cat_g1v2;

		Select 
			count(*) 
		From bronze.erp_px_cat_g1v2;

		set @batch_end_time= getdate();

		print'========================================================================================================================'
		print 'Total ingestion of data into these table takes'+ cast(datediff(second, @batch_end_time, @batch_start_time) as NVARCHAR)+' seconds';
		print'========================================================================================================================='

	END TRY

BEGIN CATCH
	PRINT'==============================================='
	print'Error occured during loading  bronze layer';
	print'Error massage'+ ERROR_MESSAGE();
	print 'Error Number'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	print'==============================================='
END CATCH

END;
