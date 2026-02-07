/*
===================================================
STORE PROCEDURE: Load 'Bronze' Layer [source --> bronze]
---------------------------------------------------
Script Purpose: 
	This SQL script is designed to automate the Data Ingestion process for
	Data Warehouse's Bronze Layer. It creates a reusable Stored Procedure 
	that cleans (truncates) and reloads all the CRM and ERP data from local 
	CSV files into SQL Server.
  It Perform the following actions: 
    - Truncate the bronze tables before loading the data 
    - Uses 'BULK INSERT' command to load data from external csv files
    - Track the process duration for each load
Parameters: 
  This stored procedure doesn't take any parametes or retun any values
Usage Example: 
  EXEC bronze.load_bronze;
====================================================
*/
-- STORE procedure to load data to bronze layer
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	-- Variable declared for calculating time duration to load each table and batch duration
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	SET @batch_start_time = GETDATE();
	-- Error Handling
	BEGIN TRY
	PRINT '------------------------------------------'
	PRINT 'LOADING BRONZE LAYER'
	PRINT '------------------------------------------'
	PRINT '============================================='
	PRINT 'Loading CRM source system'
	PRINT '============================================='
		----------------------------------------------------------------
		-- 1. LOAD data from crm_customer_info.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncating Table: bronze.crm_customer_info'
		TRUNCATE TABLE bronze.crm_customer_info;
		PRINT '...Inserting Data: bronze.crm_customer_info'
		BULK INSERT bronze.crm_customer_info
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_crm\crm_customer_info.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '\n',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 2. LOAD data from crm_product_data.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table:bronze.crm_product_data'
		TRUNCATE TABLE bronze.crm_product_data;
		PRINT 'Insert Data: crm_product_data'
		BULK INSERT bronze.crm_product_data
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_crm\crm_product_data.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'

		----------------------------------------------------------------
		-- 3. LOAD data from crm_sales_order.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table:bronze.crm_sales_order'
		TRUNCATE TABLE bronze.crm_sales_order;
		PRINT '...Insert Data: crm_sales_order'
		BULK INSERT bronze.crm_sales_order
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_crm\crm_sales_order.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 4. LOAD data from erp_category_map.csv 
		----------------------------------------------------------------
			SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table: bronze.erp_category_map'
		TRUNCATE TABLE bronze.erp_category_map;
		PRINT '...Insert Data: bronze.erp_category_map'
		BULK INSERT bronze.erp_category_map
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_category_map.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 5. LOAD data from erp_product_specs.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table:bronze.erp_product_specs'
		TRUNCATE TABLE bronze.erp_product_specs;
		PRINT '...Insert Data: erp_product_specs'
		BULK INSERT bronze.erp_product_specs
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_product_specs.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 6. LOAD data from erp_stores.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table:bronze.erp_stores'
		TRUNCATE TABLE bronze.erp_stores;
		PRINT '...Insert Data: erp_stores'
		BULK INSERT bronze.erp_stores
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_stores.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		);
				SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 7. LOAD data from erp_spatial_data.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table: bronze.erp_spatial_data'
		TRUNCATE TABLE bronze.erp_spatial_data;
		PRINT '...Insert Data: erp_spatial_data'
		BULK INSERT bronze.erp_spatial_data
		FROM 'E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_spatial_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a', -- This represents the \n character
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		----------------------------------------------------------------
		-- 8. LOAD data from erp_territory.csv 
		----------------------------------------------------------------
		SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table: bronze.erp_territory'
		TRUNCATE TABLE bronze.erp_territory;
		PRINT '...Insert Data: bronze.erp_territory'
		BULK INSERT bronze.erp_territory
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_territory.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
			----------------------------------------------------------------
		-- 7. LOAD data from erp_returns.csv 
		----------------------------------------------------------------
			SET @start_time = GETDATE();
		-- TRUNCATE the table first otherwise the data will be stored every time whenever running the code and create duplicate.
		PRINT '...Truncate Table:bronze.erp_returns'
		TRUNCATE TABLE bronze.erp_returns;
		PRINT '...Insert Data: erp_returns'
		BULK INSERT bronze.erp_returns
		FROM "E:\Data Analyst Portfolio Project\zenith-global\datasets\source_erp\erp_returns.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR= '0x0a',
			TABLOCK
		); 
		SET @end_time = GETDATE();
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '********************'
		SET @batch_end_time = GETDATE();
		PRINT '>>>Bronze Layer Loading Completed'
		PRINT '>>>LOADING Duration: ' + CAST(DATEDIFF(second, @start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT '...ERROR OCCURED WHILE LOADING BRONZE LAYER..'
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATUS:' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END;
