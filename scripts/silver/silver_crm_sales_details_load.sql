-- =============================================
-- Script: Load Cleaned Sales Data into Silver Layer
-- Target Table: silver.crm_sales_details
-- Source Table: bronze.crm_sales_details
-- Purpose:
--   1. Clean malformed dates
--   2. Ensure price, quantity, and sales values follow business rules
--   3. Handle missing or invalid values with derived logic
-- =============================================

USE DataWarehouse;

-- STEP 1: Insert cleaned and transformed data into silver.crm_sales_details
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	-- STEP 1a: Clean order date — set to NULL if invalid or incorrectly formatted
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,

	-- STEP 1b: Clean ship date
	CASE 
		WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,

	-- STEP 1c: Clean due date
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,

	-- STEP 1d: Derive sales if invalid
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,

	-- STEP 1e: Pass quantity as-is
	sls_quantity,

	-- STEP 1f: Derive price if invalid
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price

FROM bronze.crm_sales_details;

-- =============================================
-- Sanity Checks (Post Insert)
-- =============================================

-- CHECK 1: Unwanted leading/trailing spaces in order numbers
-- Expected: 0 rows
-- SELECT *
-- FROM silver.crm_sales_details
-- WHERE TRIM(sls_ord_num) != sls_ord_num;

-- CHECK 2: Ensure all product keys exist in crm_prd_info
-- SELECT *
-- FROM silver.crm_sales_details
-- WHERE sls_prd_key NOT IN (
--     SELECT prd_key FROM silver.crm_prd_info
-- );

-- CHECK 3: Ensure all customer IDs exist in crm_cust_info
-- SELECT *
-- FROM silver.crm_sales_details
-- WHERE sls_cust_id NOT IN (
--     SELECT cst_id FROM silver.crm_cust_info
-- );

-- CHECK 4: Identify any invalid dates in ship date field
-- (e.g., length != 8 or future dates beyond reasonable limit)
-- SELECT DISTINCT
--     NULLIF(sls_ship_dt, 0) AS sls_ship_dt
-- FROM silver.crm_sales_details
-- WHERE LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20500101;

-- CHECK 5: Business logic — order date should not be after ship or due dates
-- SELECT *
-- FROM silver.crm_sales_details
-- WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- CHECK 6: Business logic — ensure valid sales = quantity × price and no nulls or negative values
-- SELECT DISTINCT
--     sls_sales,
--     sls_quantity,
--     sls_price
-- FROM silver.crm_sales_details
-- WHERE sls_sales != sls_quantity * sls_price
--     OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
--     OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
-- ORDER BY sls_sales, sls_quantity, sls_price;

-- CHECK 7: Derivation logic for invalid values — preview corrections
-- SELECT DISTINCT
--     sls_sales,
--     sls_quantity,
--     sls_price,
--     CASE 
--         WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
--             THEN sls_quantity * ABS(sls_price)
--         ELSE sls_sales
--     END AS sls_new_sales,
--     CASE 
--         WHEN sls_price IS NULL OR sls_price <= 0
--             THEN sls_sales / NULLIF(sls_quantity, 0)
--         ELSE sls_price
--     END AS sls_new_price
-- FROM silver.crm_sales_details
-- WHERE sls_sales != sls_quantity * sls_price;
