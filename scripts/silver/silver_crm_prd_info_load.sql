-- STEP 0: Raw data (source) was originally checked from bronze.crm_prd_info
-- SELECT * FROM bronze.crm_prd_info;

-- STEP 1: Insert clean and transformed data into silver.crm_prd_info
-- - Transform prd_key into category_id
-- - Extract prd_key suffix
-- - Replace NULL cost with 0
-- - Standardize product line
-- - Fix date formatting and infer prd_end_dt from next start_dt
INSERT INTO silver.crm_prd_info (
	prd_id,
	category_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	-- Extract and reformat the category ID from prd_key
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS category_id,
	
	-- Extract the actual product key from prd_key
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	
	-- Keep product name as-is
	prd_nm,
	
	-- Replace NULL product cost with 0
	ISNULL(prd_cost, 0) AS prd_cost,
	
	-- Standardize product line labels
	CASE UPPER(TRIM(prd_line)) 
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	
	-- Ensure start date is in DATE format
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	
	-- Infer end date as one day before the next start date for the same product
	CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
		AS DATE
	) AS prd_end_dt
FROM bronze.crm_prd_info;

-- STEP 2: Sanity Check for unwanted spaces in product names (prd_nm)
-- Expected: No results
-- SELECT prd_nm
-- FROM silver.crm_prd_info
-- WHERE prd_nm != TRIM(prd_nm);

-- STEP 3: Sanity Check for NULL or negative prd_cost values
-- Expected: No results; if any, investigate or clean manually
-- SELECT prd_cost
-- FROM silver.crm_prd_info
-- WHERE prd_cost < 0 OR prd_cost IS NULL;

-- STEP 4: Check all distinct product lines to ensure standardization worked
-- SELECT DISTINCT prd_line
-- FROM silver.crm_prd_info;

-- STEP 5: Check for invalid date orders (prd_end_dt before prd_start_dt)
-- SELECT *
-- FROM silver.crm_prd_info
-- WHERE prd_end_dt < prd_start_dt;

-- STEP 6: Verify how prd_end_dt was inferred
-- This was useful in testing individual prd_key records before batch insertion
-- SELECT 
-- 	prd_id,
-- 	prd_key,
-- 	prd_nm,
-- 	prd_start_dt,
-- 	prd_end_dt,
-- 	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt_test
-- FROM silver.crm_prd_info
-- WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- STEP 7: Reference: Compare against ERP category keys for format consistency
-- SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2;

-- STEP 8: Optional filter when checking product keys in sales data
-- SELECT sls_prd_key FROM bronze.crm_sales_details
-- WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
--     SELECT sls_prd_key
--     FROM bronze.crm_sales_details
--     WHERE sls_prd_key LIKE 'FK%'
-- );
