--USE DataWarehouse;
/*
-- ============================================
-- STEP 1: Check for Nulls or Duplicates in Primary Key
-- ============================================

-- Check if there are any NULL or duplicate cst_id values
SELECT cst_id, COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Example: View duplicate rows for one specific ID
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- ============================================
-- STEP 2: Deduplicate and Keep Only the Most Recent Record
-- Using ROW_NUMBER() to isolate latest record per customer
-- ============================================

SELECT *
FROM (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY cst_id
			ORDER BY cst_create_date DESC
		) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1;


-- ============================================
-- STEP 3: Check for unwanted leading/trailing spaces
-- Useful for identifying dirty data that should be cleaned
-- ============================================

-- First name with extra spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Last name with extra spaces
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Gender with extra spaces
SELECT cst_gender
FROM bronze.crm_cust_info
WHERE cst_gender != TRIM(cst_gender);


-- ============================================
-- STEP 4: Check for data consistency in categorical fields
-- Ensures we map values consistently in transformation
-- ============================================

-- Gender values
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;

-- Marital status values
SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info;

*/

-- ============================================
-- FINAL STEP: Insert cleaned, deduplicated data into silver layer
-- ============================================

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date
)
SELECT
	cst_id,
	cst_key,
	
	-- Clean up whitespace
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	
	-- Normalize marital status to readable values
	CASE 
		WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END AS cst_marital_status,

	-- Normalize gender to readable values
	CASE 
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END AS cst_gender,
	
	cst_create_date
FROM (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY cst_id
			ORDER BY cst_create_date DESC
		) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) AS t
WHERE flag_last = 1; -- Select the most recent record per customer (remove duplicates)


