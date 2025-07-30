USE DataWarehouse;

BULK INSERT bronze.crm_cust_info
FROM '/Users/pragnyavijayan/Projects/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
WITH (
	FIRSTROW = 2, -- First row is the header, so exclude
	FIELDTERMINATOR = ',',
	TABLOCK
);
