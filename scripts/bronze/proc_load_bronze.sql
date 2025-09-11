/*
This Script is a Stored Procedure to run ETL process for the Bronze Layer
*/



USE [DataWareHouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze]    Script Date: 9/11/2025 12:51:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [bronze].[load_bronze] AS
BEGIN
	DECLARE @layer_start_time DATETIME, @layer_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		SET @layer_start_time = GETDATE();

		PRINT '===============================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================';

		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		  FROM
		'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		  WITH
			(
			  firstrow =2,
			  fieldterminator = ',',
			  tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';



		PRINT '-------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Old Laptop stuff\SQL_Data_Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			)
		;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';

		PRINT '----------------';

		SET @layer_end_time= GETDATE();
		PRINT 'Bronze Layer Load Duration: ' + CAST(DATEDIFF(millisecond,@layer_start_time,@layer_end_time) AS NVARCHAR)+ ' milliseconds.';

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
