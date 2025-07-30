CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    BEGIN TRY
        SET NOCOUNT OFF;

        DECLARE @start_time DATETIME, @end_time DATETIME;

        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
        RAISERROR('Loading Bronze Layer', 0, 1) WITH NOWAIT;
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;

        RAISERROR('----------------------------------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading CRM Tables', 0, 1) WITH NOWAIT;
        RAISERROR('----------------------------------------------------------------', 0, 1) WITH NOWAIT;

        -- CRM Customer Info
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_cust_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_cust_info
        FROM '/data/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.crm_cust_info = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- CRM Product Info
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISERROR('>> Inserting Data Into: bronze.crm_prd_info', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_prd_info
        FROM '/data/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.crm_prd_info = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- CRM Sales Details
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.crm_sales_details', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISERROR('>> Inserting Data Into: bronze.crm_sales_details', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.crm_sales_details
        FROM '/data/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.crm_sales_details = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        RAISERROR('----------------------------------------------------------------', 0, 1) WITH NOWAIT;
        RAISERROR('Loading ERP Tables', 0, 1) WITH NOWAIT;
        RAISERROR('----------------------------------------------------------------', 0, 1) WITH NOWAIT;

        -- ERP LOC A101
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISERROR('>> Inserting Data Into: bronze.erp_loc_a101', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_loc_a101
        FROM '/data/source_erp/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.erp_loc_a101 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ERP Customer AZ12
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISERROR('>> Inserting Data Into: bronze.erp_cust_az12', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_cust_az12
        FROM '/data/source_erp/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.erp_cust_az12 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- ERP PX_CAT_G1V2
        SET @start_time = GETDATE();
        RAISERROR('>> Truncating Table: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISERROR('>> Inserting Data Into: bronze.erp_px_cat_g1v2', 0, 1) WITH NOWAIT;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/data/source_erp/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: bronze.erp_px_cat_g1v2 = ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
        RAISERROR('Finished Loading Bronze Layer', 0, 1) WITH NOWAIT;
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;

    END TRY

    BEGIN CATCH
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
        RAISERROR('ERROR OCCURRED DURING LOADING BRONZE LAYER', 0, 1) WITH NOWAIT;
        --RAISERROR('Error Message: %s', 0, 1, ERROR_MESSAGE()) WITH NOWAIT;
        --RAISERROR('Error Number: %d', 0, 1, ERROR_NUMBER()) WITH NOWAIT;
        --RAISERROR('Error State: %d', 0, 1, ERROR_STATE()) WITH NOWAIT;
        --RAISERROR('Error Severity: %d', 0, 1, ERROR_SEVERITY()) WITH NOWAIT;
        --RAISERROR('Error Line: %d', 0, 1, ERROR_LINE()) WITH NOWAIT;
        RAISERROR('================================================================', 0, 1) WITH NOWAIT;
    END CATCH;
END;
