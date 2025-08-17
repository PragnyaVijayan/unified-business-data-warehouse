CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================================';

    RAISE NOTICE '----------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '----------------------------------------------------------------';

    -- CRM Customer Info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/data/source_crm/cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.crm_cust_info = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- CRM Product Info
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/data/source_crm/prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.crm_prd_info = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- CRM Sales Details
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/data/source_crm/sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.crm_sales_details = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    RAISE NOTICE '----------------------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '----------------------------------------------------------------';

    -- ERP LOC A101
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/data/source_erp/LOC_A101.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.erp_loc_a101 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ERP Customer AZ12
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/data/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.erp_cust_az12 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    -- ERP PX_CAT_G1V2
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/data/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: bronze.erp_px_cat_g1v2 = % seconds', 
        EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER;

    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Finished Loading Bronze Layer';
    RAISE NOTICE '================================================================';

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: Entire bronze layer = % seconds', 
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time))::INTEGER;

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error Message: %', SQLERRM;
    RAISE NOTICE 'Error Context: %', SQLSTATE;
    RAISE NOTICE '================================================================';
    RAISE;
END;
$$ LANGUAGE plpgsql;
