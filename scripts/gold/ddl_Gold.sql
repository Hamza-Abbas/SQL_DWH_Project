/*
==============================================================================
DDL Script: Create Gold Views
==============================================================================
This is the final layer of the Datawarehouse and it represents the final fact
& dimension tables (Star Schema)

This gold layer creates three views: dimensions(customers and products) & fact(sales)
Each view performs transformations and combines data from the silver layer to produce
clean, structured, enriched and business-ready dataset.

These views can be directly queries for analytics and reporting.
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key
 ,c_i.cst_id AS customer_id
,c_i.cst_key AS customer_number
,c_i.cst_firstname AS first_name
,c_i.cst_lastname AS last_name
,c_loc.CNTRY AS country
,c_i.cst_marital_status AS marital_status
,CASE WHEN c_i.cst_gndr != 'n/a' THEN c_i.cst_gndr
	ELSE COALESCE(c_ad_i.GEN , 'n/a')
	END AS gender
,c_ad_i.BDATE AS birth_date
,c_i.cst_create_date AS create_date


FROM silver.crm_cust_info c_i
LEFT JOIN silver.erp_cust_az12 c_ad_i
ON c_i.cst_key = c_ad_i.CID
LEFT JOIN silver.erp_loc_a101 c_loc
ON c_i.cst_key = c_loc.CID



IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE view gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY p_i.prd_start_dt , p_i.prd_key) AS product_key
 ,p_i.prd_id AS product_id
 ,p_i.prd_key AS product_number
 ,p_i.prd_nm AS product_name
,p_i.cat_id AS category_id
,p_cat.CAT AS category
,p_cat.SUBCAT AS sub_category
,p_cat.MAINTENANCE AS maintenance
,p_i.prd_cost AS product_cost
,p_i.prd_line AS product_line
,p_i.prd_start_dt AS start_date

FROM silver.crm_prd_info AS  p_i

LEFT JOIN silver.erp_px_cat_g1v2 AS p_cat

ON p_i.cat_id = p_cat.ID

WHERE p_i.prd_end_dt IS NULL



IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS 
SELECT sd.[sls_ord_num] AS order_number
      ,pr.product_key
      ,cu.customer_key
      ,sd.[sls_order_dt] AS order_date
      ,sd.[sls_ship_dt] AS shipping_date
      ,sd.[sls_due_dt] AS due_date
      ,sd.[sls_sales] AS sales_amount
      ,sd.[sls_quantity] AS quantity
      ,sd.[sls_price] AS price

  FROM [silver].[crm_sales_details] sd
  LEFT JOIN [gold].[dim_products] pr
  ON sd.sls_prd_key = pr.product_number
  LEFT JOIN gold.dim_customers cu
  ON sd.sls_cust_id = cu.customer_id

