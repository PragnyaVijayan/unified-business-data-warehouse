/* 
 * Function: silver.load_silver()
 * Description: Transforms and loads data from bronze layer to silver layer
 * Usage: SELECT silver.load_silver();
 */

CREATE OR REPLACE FUNCTION silver.load_silver()
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    -- Batch start log
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================================';

    batch_start_time := CURRENT_TIMESTAMP;

    -- ================================
    -- Load CRM Customer Information
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.crm_cust_info';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.crm_cust_info;

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
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
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
    WHERE flag_last = 1;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.crm_cust_info = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Load CRM Product Information
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.crm_prd_info';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.crm_prd_info;

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
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
        SUBSTRING(prd_key FROM 7),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt::DATE,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.crm_prd_info = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Load CRM Sales Details
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.crm_sales_details';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.crm_sales_details;

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
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.crm_sales_details = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Load ERP Location Info
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.erp_loc_a101';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(REGEXP_REPLACE(cntry, E'[\r\n\x0A]', '', 'g')) = 'DE' THEN 'Germany'
            WHEN TRIM(REGEXP_REPLACE(cntry, E'[\r\n\x0A]', '', 'g')) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(REGEXP_REPLACE(cntry, E'[\r\n\x0A]', '', 'g')) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(REGEXP_REPLACE(cntry, E'[\r\n\x0A]', '', 'g'))
        END
    FROM bronze.erp_loc_a101;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.erp_loc_a101 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Load ERP Category Info
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.erp_px_cat_g1v2';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id, 
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.erp_px_cat_g1v2 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Load ERP Customer Info
    -- ================================
    RAISE NOTICE '>> Loading Table: silver.erp_cust_az12';
    start_time := CURRENT_TIMESTAMP;

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE 
            WHEN UPPER(TRIM(REGEXP_REPLACE(gen, E'[\r\n\x0A]', '', 'g'))) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(REGEXP_REPLACE(gen, E'[\r\n\x0A]', '', 'g'))) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: silver.erp_cust_az12 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ================================
    -- Completion Logs
    -- ================================
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Finished Loading Silver Layer';
    RAISE NOTICE '================================================================';

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Total Duration: silver layer = % seconds', 
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Context: %', SQLSTATE;
    RAISE NOTICE '================================================================';
    RAISE;
END;
$$ LANGUAGE plpgsql;