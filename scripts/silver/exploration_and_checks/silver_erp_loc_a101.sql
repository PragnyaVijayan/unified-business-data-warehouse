-- Switch to the target database
USE DataWarehouse;

-- Insert cleaned and standardized data from bronze.erp_loc_a101 into silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    -- Remove dashes from cid
    REPLACE(cid, '-', '') AS cid,

    -- Standardize country codes/names
    CASE 
        WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))
    END AS cntry
FROM bronze.erp_loc_a101;


-- -- Preview the inserted data from silver schema
-- SELECT *
-- FROM silver.erp_loc_a101;


-- -- Note: The cid in erp_loc_a101 should be mapped to cst_key in silver.crm_cust_info
-- -- Verify formatting of cid without dashes
-- SELECT
--     cid,
--     cntry
-- FROM silver.erp_loc_a101
-- WHERE cid NOT LIKE '%-%';  -- Rows where cid still contains no dashes


-- -- Show cid with dashes removed, alongside country
-- SELECT
--     REPLACE(cid, '-', '') AS cid,
--     cntry
-- FROM silver.erp_loc_a101;


-- -- Find cids (with dashes removed) not existing in crm_cust_info.cst_key
-- SELECT
--     REPLACE(cid, '-', '') AS cid,
--     cntry
-- FROM silver.erp_loc_a101
-- WHERE REPLACE(cid, '-', '') NOT IN 
--     (SELECT cst_key FROM silver.crm_cust_info);


-- -- Check country data standardization and lengths
-- SELECT DISTINCT 
--     cntry,
--     LEN(cntry) AS length
-- FROM silver.erp_loc_a101
-- ORDER BY cntry; 


-- -- Re-apply country cleaning logic to bronze table for comparison,
-- -- to check if TRIM or REPLACE are causing any issues
-- SELECT DISTINCT 
--     CASE 
--         WHEN LTRIM(RTRIM(REPLACE(cntry, CHAR(13), ''))) = 'DE' THEN 'Germany'
--         WHEN LTRIM(RTRIM(REPLACE(cntry, CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
--         WHEN LTRIM(RTRIM(REPLACE(cntry, CHAR(13), ''))) = '' OR cntry IS NULL THEN 'n/a'
--         ELSE LTRIM(RTRIM(REPLACE(cntry, CHAR(13), '')))
--     END AS cntry_modified
-- FROM bronze.erp_loc_a101
-- ORDER BY cntry_modified; 

-- -- Note: TRIM may not work as expected due to special or non-breaking spaces;
-- -- consider using additional REPLACE for CHAR(160) or other whitespace characters.
