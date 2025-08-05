/* 
 * Stored Procedure: silver.load_silver
 * Description: Transforms and loads data from bronze layer to silver layer
 * Usage: EXEC silver.load_silver;
 */

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    -- Batch start log
    RAISERROR('================================================================', 0, 1) WITH NOWAIT;
    RAISERROR('Loading Silver Layer', 0, 1) WITH NOWAIT;
    RAISERROR('================================================================', 0, 1) WITH NOWAIT;

    BEGIN TRY
        SET @batch_start_time  = GETDATE();

        -- ================================
        -- Load CRM Customer Information
        -- ================================
        RAISERROR('>> Loading Table: silver.crm_cust_info', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.crm_cust_info = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Load CRM Product Information
        -- ================================
        RAISERROR('>> Loading Table: silver.crm_prd_info', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                AS DATE
            )
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.crm_prd_info = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Load CRM Sales Details
        -- ================================
        RAISERROR('>> Loading Table: silver.crm_sales_details', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

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
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.crm_sales_details = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Load ERP Location Info
        -- ================================
        RAISERROR('>> Loading Table: silver.erp_loc_a101', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', ''),
            CASE 
                WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) = 'DE' THEN 'Germany'
                WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), '')) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))
            END
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.erp_loc_a101 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Load ERP Category Info
        -- ================================
        RAISERROR('>> Loading Table: silver.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.erp_px_cat_g1v2 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Load ERP Customer Info
        -- ================================
        RAISERROR('>> Loading Table: silver.erp_cust_az12', 0, 1) WITH NOWAIT;
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END,
            CASE 
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(160), ''))) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: silver.erp_cust_az12 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ================================
        -- Completion Logs
        -- ================================
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
        RAISERROR('Finished Loading Silver Layer', 0, 1) WITH NOWAIT;
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;

        SET @batch_end_time = GETDATE();
        PRINT '>> Total Duration: silver layer = ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

    END TRY

    BEGIN CATCH
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING SILVER LAYER', 0, 1) WITH NOWAIT;
        -- Uncomment for detailed error diagnostics:
        -- RAISERROR('Error Message: %s', 0, 1, ERROR_MESSAGE()) WITH NOWAIT;
        -- RAISERROR('Error Number: %d', 0, 1, ERROR_NUMBER()) WITH NOWAIT;
        -- RAISERROR('Error State: %d', 0, 1, ERROR_STATE()) WITH NOWAIT;
        -- RAISERROR('Error Severity: %d', 0, 1, ERROR_SEVERITY()) WITH NOWAIT;
        -- RAISERROR('Error Line: %d', 0, 1, ERROR_LINE()) WITH NOWAIT;
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
    END CATCH;
END;
