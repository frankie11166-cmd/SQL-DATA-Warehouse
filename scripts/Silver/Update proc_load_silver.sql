Store procedure: Load Silver layer (bronze to Silver)
------------------------------------------------------
Purpose: 
This store procedure performs the ETL(Extract, Transform, Load) process to populate the 'Silver' shcema from the 'Bronze' schema.
------------------------------------------------------
Actions performed:
--Truncates silver tables.
--Inserts transformed and cleansed data from Bronze into Silver table.TABLE



--Process from get all raw data files to cleaned data in silver layer. 
EXEC bronze.load_bronze
EXEC silver.load_silver 



CREATE or ALTER PROCEDURE silver.load_silver AS
BEGIN  
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
   BEGIN TRY

    set @batch_start_time = GETDATE();
    SET @start_time = GETDATE();

    --1.
    PRINT '--Truncating table: silver.crm_cust_info '
    TRUNCATE TABLE silver.crm_cust_info
    PRINT '--Inserting Data Into: silver.crm_cust_info'
    INSERT into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    )
    --either the column in bronze layer matches the name in silver layer, or using AS
    --AS cst_id → tells SQL that est_id in Bronze maps to cst_id in Silver


    --Row Number(), assign a unique number to each row based on the defined order. 
    --duplicated cst_id only appear the freshest one.  
    select 
        cst_id,
        cst_key,
        TRIM(cst_firstname) as cst_firstname,
        TRIM(cst_lastname) as cst_lastname,
        case when UPPER(TRIM(cst_material_status)) = 'S' then 'Single'
            when UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
            else 'N/A'
            END cst_material_status,
        case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
            when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
            else 'N/A'
            END cst_gndr,
        cst_create_date
    from (
        SELECT 
            * ,
            ROW_NUMBER() OVER (PARTITION by cst_id ORDER by cst_create_date desc) as flag_last
        from bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    )t
    WHERE flag_last = 1

    set @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) as NVARCHAR)+ ' seconds'
    PRINT '---------------------------------------'

      
    --2.
    --SQL Server mechanism:
    --SELECT pulls rows from Bronze
    --SELECT transforms rows (substring, replace, case, cast, lead)
    --INSERT inserts them into Silver

    set @start_time = GETDATE();

    PRINT '--Truncating table: silver.crm_prd_info '
    TRUNCATE TABLE silver.crm_prd_info
    PRINT '--Inserting Data Into: silver.crm_prd_info'

    INSERT into silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )

    SELECT 
        prd_id,
        replace(SUBSTRING(prd_key, 1, 5),'-','_') as cat_id,
        SUBSTRING(prd_key, 7,LEN(prd_key)) as prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) as prd_cost,
        case when UPPER(prd_line) = 'M' then 'Mountain'
            when UPPER(prd_line) = 'R' then 'Road'
            when UPPER(prd_line) = 'S' then 'other sales'
            when upper(prd_line) = 'T' then 'Touring'
            else 'n/a'
        END AS prd_line,
        cast(prd_start_dt as date) as prd_start_dt,
        cast(
            lead(prd_start_dt) OVER (PARTITION by prd_key ORDER by prd_start_dt) -1 
            as DATE) AS prd_end_dt1
    from bronze.crm_prd__info

    set @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'

    --3.
    set @start_time = GETDATE();
    PRINT '--Truncating table: silver.crm_sales_details '
    TRUNCATE TABLE silver.crm_sales_details
    PRINT '--Inserting Data Into:silver.crm_sales_details'

    INSERT into silver.crm_sales_details(
    sls_ord_num ,
    sls_prd_key,
    sis_cust_id   ,
    sls_order_dt ,
    sls_ship_dt  ,
    sls_due_dt   ,
    sls_sales      ,
    sls_quantity   ,
    sls_price  
    )

    SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then NULL
        else CAST(cast(sls_order_dt as varchar) as DATE)  
    END as sls_order_dt,

    case when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then NULL
        else CAST(cast(sls_ship_dt as varchar) as DATE)  
    END as sls_ship_dt,

    case when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then NULL
        else CAST(cast(sls_due_dt as varchar) as DATE)  
    END as sls_due_dt,

    case when sls_sales is NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    Case when sls_price is null or sls_price <= 0 
        then sls_sales / nullif(sls_quantity,0)
        ELSE sls_price 
    END AS sls_price

    FROM bronze.crm_sales_details 

    set @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
    PRINT '---------------------------------------'


    --4.
    PRINT '--Truncating table:  silver.erp_cust_az12 '
    TRUNCATE TABLE  silver.erp_cust_az12
    PRINT '--Inserting Data Into:  silver.erp_cust_az12'

    SET @start_time = GETDATE();
    insert into silver.erp_cust_az12(
    cid,bdate,gen
    )

    --NAS% means if is like NAS in the string, remove excessive value
    SELECT 
    case when cid like 'NAS%' then SUBSTRING(cid, 4,LEN(cid))
        else cid
    end as cid,
    --handle birthday
    case when bdate > GETDATE() then NULL
        else bdate
    end as bdate,
    --CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
        --WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male' 
        --ELSE 'n/a' 
    --END as gen,
    gen
    FROM bronze.erp_cust_az12
    SET @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds' 
    PRINT '---------------------------------------'

    --5.
        SET @start_time = GETDATE();
    PRINT '--Truncating table: silver.erp_loc_a101 '
    TRUNCATE TABLE silver.erp_loc_a101
    PRINT '--Inserting Data Into:silver.erp_loc_a101'
    insert into silver.erp_loc_a101(
    cid,cntry
    )

    SELECT
    --remove excessive value 
    REPLACE(cid, '-','')cid,
    case when TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) in ('US','USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
    from bronze.erp_loc_a101
    SET @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds' 
    PRINT '---------------------------------------'

    --6.
        SET @start_time = GETDATE();
    PRINT '--Truncating table: silver.erp_px_cat_g1v2 '
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    PRINT '--Inserting Data Into:silver.erp_px_cat_g1v2'

    insert into silver.erp_px_cat_g1v2(
    id, cat,subcat, maintenance
    )

    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2
    SET @end_time = GETDATE();
    PRINT 'Loading duration ' + cast(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds' 
    PRINT '---------------------------------------'

    SET @batch_end_time = GETDATE();
    PRINT 'Total loading duration ' + cast(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'

    END TRY
    BEGIN CATCH 
    PRINT 'Error occured during loading Silver layer'
    PRINT 'Error message' + Error_message()
    PRINT 'Error message' + Cast(Error_number() AS NVARCHAR)
    PRINT 'Error message' + Cast(error_state() AS NVARCHAR)
    END CATCH
END
