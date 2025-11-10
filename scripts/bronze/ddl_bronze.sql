/*=============================================================
  Project:  Data Warehousing with SQL Server
  Author:   MD ALTAF HOSSAIN SUNNY
  File:     bronze_layer_tables.sql
  Purpose:  Create Bronze layer tables for raw CSV ingestion
===============================================================

  ??  WARNING:
  --------------------------------------------------------------
  - This script will DROP existing tables in the 'bronze' schema
    if they already exist in your database.
  - All data in those tables will be permanently deleted.
  - Only execute this script when you want to reset your Bronze
    layer structure or during initial setup.
  --------------------------------------------------------------
==============================================================*/

--=============================================================
-- Step 1: Use the DataWarehouse database
--=============================================================
USE DataWarehouse;
GO

/*=============================================================
  ?? CRM CUSTOMER INFORMATION TABLE
=============================================================*/
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO

/*=============================================================
  ?? CRM PRODUCT INFORMATION TABLE
=============================================================*/
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line CHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE
);
GO

/*=============================================================
  ?? CRM SALES DETAILS TABLE
=============================================================*/
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO

/*=============================================================
  ?? ERP CUSTOMER AZ12 TABLE
=============================================================*/
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);
GO

/*=============================================================
  ?? ERP LOCATION A01 TABLE
=============================================================*/
IF OBJECT_ID('bronze.erp_loc_a01', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a01;
GO

CREATE TABLE bronze.erp_loc_a01 (
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50)
);
GO

/*=============================================================
  ?? ERP PRODUCT CATEGORY G1V2 TABLE
=============================================================*/
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);
GO

--=============================================================
-- ? All Bronze layer tables created successfully
--=============================================================
PRINT '? Bronze layer tables created successfully!';


