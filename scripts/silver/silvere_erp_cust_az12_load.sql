-- USE DataWarehouse;

-- Step 1: Preview current contents of the silver table
-- SELECT * FROM silver.erp_cust_az12;

-- Step 2: Insert clean and validated data into silver.erp_cust_az12 from bronze.erp_cust_az12
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
SELECT
	-- Clean `cid`: remove 'NAS' prefix if present
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,

	-- Validate `bdate`: replace future dates with NULL
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,

	-- Normalize `gen`: clean up hidden characters and standardize values
	CASE 
		WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- Step 3: Check for any NULL values in `cid` after transformation
-- SELECT * FROM silver.erp_cust_az12 WHERE cid IS NULL;

-- Step 4: Confirm `cid` formatting consistency by comparing against known formats (e.g., from crm_cust_info)
-- SELECT * FROM silver.erp_cust_az12 WHERE cid LIKE '%AW00011000%';

-- SELECT * FROM silver.crm_cust_info;

-- Step 5: Check if cleaned `cid`s exist in silver.crm_cust_info to identify unmatched records
-- SELECT
-- 	cid,
-- 	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END AS cleaned_cid,
-- 	bdate,
-- 	gen
-- FROM silver.erp_cust_az12
-- WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END
-- NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Step 6: Validate `bdate` values for plausibility (e.g., check for outliers)
-- SELECT DISTINCT bdate FROM silver.erp_cust_az12
-- WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Step 7: Examine distinct values in `gen` for inconsistencies
-- SELECT DISTINCT gen FROM silver.erp_cust_az12;

-- Step 8: Preview how raw values in `gen` will map after normalization
-- SELECT DISTINCT
-- 	gen,
-- 	CASE 
-- 		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
-- 		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
-- 		ELSE 'n/a'
-- 	END AS gen_new
-- FROM silver.erp_cust_az12;

-- Step 9: Check for hidden characters that interfere with trimming
-- SELECT DISTINCT gen, LEN(gen) AS len, DATALENGTH(gen) AS datalength FROM silver.erp_cust_az12;

-- Step 10: Final test - clean up non-printable characters explicitly and recheck `gen` normalization
-- SELECT DISTINCT
-- 	gen,
-- 	CASE 
-- 		WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('F', 'FEMALE') THEN 'Female'
-- 		WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('M', 'MALE') THEN 'Male'
-- 		ELSE 'n/a'
-- 	END AS gen_new
-- FROM silver.erp_cust_az12;
