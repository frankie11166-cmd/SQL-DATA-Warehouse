------------------------------------------------
Quality Checks
------------------------------------------------
Script Purpose:
This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schemas. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks
------------------------------------------------


--------THIS IS PAGE OF CLEANING bronze.crm_sales_details 

--if it is 0, turn it to NULL, otherwise keep the value, and unreasonable date 
SELECT
NULLIF (sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101


--check any space in the front or end.
SELECT sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--check if all prd_key in the table of prd_key. 
 SELECT COUNT(*) FROM bronze.crm_sales_details
 WHERE sls_prd_key  IN (select prd_key from silver.crm_prd_info)

--check any order day is earilier than the shipping day. 
SELECT * FROM 
silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT * FROM 
silver.crm_sales_details

--if value is 0, return null
NullIF(Value, 0)

--if value is NULL, return 0
ISNULL(Value,0)


--check sales, quantity and price, follow business rules.
SELECT distinct
sls_sales as old_sls_sales, 
sls_quantity,
sls_price as old_sls_price,

case when sls_sales is NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
  THEN sls_quantity * ABS(sls_price)
  ELSE sls_sales
END AS sls_sales,

Case when sls_price is null or sls_price <= 0 
    then sls_sales / nullif(sls_quantity,0)
    ELSE sls_price 
END AS sls_price

from bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL or sls_quantity IS null or sls_price is NULL
or sls_sales <= 0 OR sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price




--Clean bronze.erp_cust_az12
--birthday check, bigger than the current date. 
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

--check distinct of Gen
SELECT distinct 
gen ,
case 
     when UPPER(TRIM(gen)) in ('F','Female') THEN 'Female'
     when UPPER(TRIM(gen)) in ('M','Male') THEN 'Male' 
     ELSE 'n/a' 
END as gen
from bronze.erp_cust_az12


--clean bronze.erp_loc_a101 and bronze.erp_px_cat_g1v2

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



select distinct
cntry as old_cntry,
case when TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) in ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER by cntry



--checked duplicated ID
SELECT cst_id,
COUNT(*)
from silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1


--check unwwanted space in silver layer. 
select cst_firstname
from silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)


select prd_line
from bronze.crm_prd__info
where prd_line != TRIM(prd_line)

--check standardization & Consistency, which mean whether one is expanded to two. 
--EG: 'Male', is splited to 'M', 'MALE'
select distinct cst_gndr
FROM silver.crm_cust_info

SELECT prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is NULL

select distinct prd_line
FROM silver.crm_prd_info

SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt 
