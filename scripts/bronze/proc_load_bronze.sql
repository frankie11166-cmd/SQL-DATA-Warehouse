/*

Stored Procedure: Load Bronze Layer (Source -> Bronze)

Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.


Usage Example:
  EXEC bronze.load_bronze

-----------------------------------------------------------------------------------

CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
   DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY

        SET @batch_start_time = GETDATE();

        print '=================='
        print 'Loading bronze layer'
        print '=================='

        PRINT '-----------------'
        PRINT 'Loading CRM Tables'
        PRINT '-----------------'

--1.
    set @start_time = GETDATE();
        PRINT '--Truncate table: bronze.crm_cust_info'
        
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'Inserting data into: bronze.crm_cust_inf '
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );
    set @end_time = GETDATE();
    PRINT 'Load Duration: ' + cast (DATEDIFF (SECOND,@start_time, @end_time) as NVARCHAR )  + ' Seconds'
--string + int + string   ❌ not allowed, string + string + string   ✅ allowed


--2.
 SET @start_time = GETDATE();
    PRINT '-----------------'
    PRINT '--Truncate table: bronze.crm_prd__info'
    PRINT 'Inserting data into: bbronze.crm_prd__info '
       
        TRUNCATE TABLE bronze.crm_prd__info;

        BULK INSERT bronze.crm_prd__info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );
   SET @end_time = GETDATE();
   PRINT 'Loading duration' + cast ( DATEDIFF (SECOND, @start_time, @end_time) as NVARCHAR ) + ' Seconds'
--@start_time and @end_time. always calculate the closest one. 


--3.
    PRINT '-----------------'
    PRINT '--Truncate table: bronze.crm_sales_details'
    PRINT 'Inserting data into: bronze.crm_sales_details '
        
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );


    PRINT '-----------------'
    PRINT '--Truncate table: bronze.erp_cust_az12'
    PRINT 'Inserting data into: bronze.erp_cust_az12 '
        --4.
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/cust_az12.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );

    PRINT '-----------------'
    PRINT '--Truncate table: bronze.erp_LOC_A101'
    PRINT 'Inserting data into: bronze.erp_LOC_A101 '
        --5.
        TRUNCATE TABLE bronze.erp_LOC_A101;

        BULK INSERT bronze.erp_LOC_A101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );

    PRINT '-----------------'
    PRINT '--Truncate table: bronze.erp_PX_CAT_G1V2'
    PRINT 'Inserting data into: bronze.erp_PX_CAT_G1V2 '
        --6.
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            firstrow = 2,
            fieldterminator = ',',
            tablock 
        );

    set @batch_end_time = GETDATE();
    PRINT 'Loading bronze layer duration' + cast(DATEDIFF(Second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'


   END TRY
   BEGIN CATCH 
        PRINT '-------------------------'
        PRINT 'Error occured during loading Bronze Layer'
        PRINT 'Error Message' + ERROR_MESSAGE();
        PRINT 'Error Message' + CAST(ERROR_NUMBER()AS nvarchar);
        PRINT 'Error Message' + CAST(ERROR_STATE()AS nvarchar);
        PRINT '-------------------------'
   END CATCH
END


