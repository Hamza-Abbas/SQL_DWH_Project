/*
==========================================================
Loading Cleaned Data Into Silver Layer From Bronze Layer
==========================================================

---------------------------------------------------------
This Stored Prcedure Truncates the tables in Silver Layer
Then Takes the data from Bronze Layer and Clean it
Then Insert this transformed data into Silver Layer

*/

USE [DataWareHouse]
GO
/****** Object:  StoredProcedure [silver].[load_silver]    Script Date: 9/17/2025 4:34:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [silver].[load_silver] AS
BEGIN
	DECLARE @layer_start_time DATETIME, @layer_end_time DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        SET @layer_start_time = GETDATE();
		PRINT '===============================';
		PRINT 'Loading Silver Layer';
		PRINT '===============================';

		PRINT '-------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------';

        --Table 1 : crm_cust_into
        PRINT 'Truncating Table >> silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info

        PRINT 'Inserting Data Into >> silver.crm_cust_info'

        INSERT INTO silver.crm_cust_info (
        cst_id
        ,cst_key
        ,cst_firstname
        ,cst_lastname
        ,cst_marital_status
        ,cst_gndr
        ,cst_create_date
        )
        SELECT 
        cst_id
        ,cst_key
        ,TRIM(cst_firstname) AS cst_firstname
        ,TRIM(cst_lastname) AS cst_lastname
        ,CASE 
	        WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'
	        WHEN UPPER(TRIM(cst_marital_status))= 'M' THEN 'Married'
	        ELSE 'n/a'
        END cst_marital_status
        ,CASE 
	        WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'
	        WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'Male'
	        ELSE 'n/a'
        END cst_gndr
        ,cst_create_date
        FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL) t WHERE flag_last=1 

        PRINT 'Loading Completed'
        PRINT '----------------------------------'

        --Table 2: crm_prd_info
        PRINT 'Truncating Table >> silver.crm_prd_info'
        TRUNCATE TABLE silver.crm_prd_info

        PRINT 'Inserting Data Into >> silver.crm_prd_info' 

        INSERT INTO silver.crm_prd_info (
        prd_id
        ,cat_id
        ,prd_key
        ,prd_nm
        ,prd_cost
        ,prd_line
        ,prd_start_dt
        ,prd_end_dt
        )
        SELECT [prd_id]
              ,REPLACE(SUBSTRING(prd_key , 1 ,5), '-'  ,'_') AS cat_id
              ,SUBSTRING(prd_key , 7 ,LEN(prd_key)) AS prd_key
              ,[prd_nm]
              ,ISNULL(prd_cost,0) AS [prd_cost]
              ,CASE UPPER(TRIM(prd_line))
                   WHEN 'M' THEN 'Mountain'
                   WHEN 'R' THEN 'Road'
                   WHEN 'S' THEN 'Other Sales'
                   WHEN 'T' THEN 'Touring'
                   ELSE 'n/a'
               END [prd_line]
              ,CAST([prd_start_dt] AS DATE) AS prd_start_dt
              ,CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS [prd_end_dt]
          FROM [bronze].[crm_prd_info]

        PRINT 'Loading Completed'
        PRINT '----------------------------------'


        --Table 3: crm_sales_details
        PRINT 'Truncating Table >> silver.crm_sales_details'
        TRUNCATE TABLE silver.crm_sales_details

        PRINT 'Inserting Data Into >> silver.crm_sales_details' 

        INSERT INTO silver.crm_sales_details (
        sls_ord_num
        ,sls_prd_key
        ,sls_cust_id
        ,sls_order_dt
        ,sls_ship_dt
        ,sls_due_dt
        ,sls_sales
        ,sls_quantity
        ,sls_price
        )
        SELECT 
        sls_ord_num
        ,sls_prd_key
        ,sls_cust_id
        ,CASE
	        WHEN sls_order_dt <=0  OR LEN(sls_order_dt) != 8 THEN NULL
	        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
         END AS sls_order_dt
        ,CASE
	        WHEN sls_ship_dt <=0  OR LEN(sls_ship_dt) != 8 THEN NULL
	        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
         END AS sls_ship_dt
        ,CASE
	        WHEN sls_due_dt <=0  OR LEN(sls_due_dt) != 8 THEN NULL
	        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
         END AS sls_due_dt
        ,CASE
		        WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
		        THEN sls_quantity * ABS(sls_price)
		        ELSE sls_sales
         END AS sls_sales
        ,sls_quantity
        ,CASE 
			        WHEN sls_price < 0 THEN sls_price * (-1)
			        WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
			        ELSE sls_price
         END AS  sls_price
        FROM bronze.crm_sales_details

        PRINT 'Loading Completed'
        PRINT '----------------------------------'


        PRINT '-------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------';

        --Table 4: erp_cust_az12
        PRINT 'Truncating Table >> silver.erp_cust_az12'
        TRUNCATE TABLE silver.erp_cust_az12

        PRINT 'Inserting Data Into >> silver.erp_cust_az12' 

        INSERT INTO [silver].[erp_cust_az12] (
        CID
        ,BDATE
        ,GEN
        )
        SELECT 
            CASE WHEN LEN(CID) = 13 THEN SUBSTRING(CID,4,LEN(CID))
            ELSE CID
            END AS CID
            ,CASE WHEN BDATE > GETDATE() THEN NULL
                   ELSE BDATE
             END AS BDATE
            ,CASE 
                WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END AS GEN
          FROM [bronze].[erp_cust_az12]

        PRINT 'Loading Completed'
        PRINT '----------------------------------'


        --Table 5: erp_loc_a101
        PRINT 'Truncating Table >> silver.erp_loc_a101'
        TRUNCATE TABLE silver.erp_loc_a101

        PRINT 'Inserting Data Into >> silver.erp_loc_a101' 

        INSERT INTO silver.erp_loc_a101(cid,CNTRY)
        SELECT 
            REPLACE(CID,'-','') AS CID,
            CASE 
                WHEN UPPER(TRIM(CNTRY)) IN ('US','United States','USA') THEN 'United States'
                WHEN UPPER(TRIM(CNTRY)) IN ('DE','Germany') THEN 'Germany'
                WHEN TRIM(CNTRY) IS NULL OR CNTRY='' THEN 'n/a'
                ELSE TRIM(CNTRY)
            END AS CNTRY
        FROM bronze.[erp_loc_a101]

        PRINT 'Loading Completed'
        PRINT '----------------------------------'


        PRINT 'Truncating Table >> silver.erp_px_cat_g1v2'
        TRUNCATE TABLE silver.erp_px_cat_g1v2

        PRINT 'Inserting Data Into >> silver.erp_px_cat_g1v2' 

        INSERT INTO [silver].[erp_px_cat_g1v2] (
        ID 
	        ,CAT
	        ,SUBCAT
	        ,MAINTENANCE
        )
        SELECT 
	        ID 
	        ,CAT
	        ,SUBCAT
	        ,MAINTENANCE
        FROM [bronze].[erp_px_cat_g1v2]


        PRINT 'Loading Completed'
        PRINT '----------------------------------'

        PRINT '==================================='
        PRINT 'Complete Silver Layer Loading Completed'
        PRINT '==================================='

        SET @layer_end_time = GETDATE()
        PRINT 'Silver Layer Loading Duration >> ' + CAST(DATEDIFF(second, @layer_start_time, @layer_end_time) AS NVARCHAR)
                + ' Seconds.'
    END TRY
    BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH


END
