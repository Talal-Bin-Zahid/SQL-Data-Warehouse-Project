-- Data Cleaning , processing and standardization of bronze.crm_cust_info table
-- Check For Nulls or Duplicates in Primary key 
-- Expectation : No Result 

select cst_id , COUNT(*) from bronze.crm_cust_info 
GROUP BY cst_id 
Having COUNT(*) > 1 OR cst_id is NUll ;

-- Data Transformation 
Select * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
from bronze.crm_cust_info 
where cst_id = 29466 ;

-- Selecting only 1 value 
Select * from (
Select * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
from bronze.crm_cust_info ) t
Where flag_last = 1 AND cst_id = 29466 ;

-- Check for unwanted Spaces 
-- Expectation : No Results 

Select cst_firstname 
From bronze.crm_cust_info
Where cst_firstname != Trim(cst_firstname) ;

Select cst_lastname 
From bronze.crm_cust_info
Where cst_lastname != Trim(cst_lastname) ;

Select cst_gndr 
From bronze.crm_cust_info
Where cst_gndr != Trim(cst_gndr) ;

-- Trimming Values in columns 
Select 
cst_id , 
cst_key ,
TRIM (cst_firstname) AS cst_firstname ,
TRIM (cst_lastname) AS cst_lastname ,
cst_marital_status ,
cst_gndr ,
cst_create_date 
from (
Select * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
from bronze.crm_cust_info ) t
Where flag_last = 1 ;

-- Data Standarization & Consistency 
Select Distinct cst_gndr 
From bronze.crm_cust_info ;

Select Distinct cst_marital_status 
From bronze.crm_cust_info ;

Select
cst_id , 
cst_key ,
TRIM (cst_firstname) AS cst_firstname ,
TRIM (cst_lastname) AS cst_lastname ,
Case When UPPER(TRIM(cst_marital_status)) = 'S' Then 'Single'
     When UPPER(TRIM(cst_marital_status)) = 'M' Then 'Married'
     Else 'n/a'
END cst_marital_status,
Case When UPPER(TRIM(cst_gndr)) = 'F' Then 'Female'
     When UPPER(TRIM(cst_gndr)) = 'M' Then 'Male'
     Else 'n/a'
END cst_gndr,
cst_create_date 
from (
Select * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
from bronze.crm_cust_info ) t
Where flag_last = 1 ;

-- Adding the data in Silver Table 
Print '>> Truncating Table : silver.crm_cust_info ' ;
Truncate Table silver.crm_cust_info ;
Print '>> Inserting Data Into : silver.crm_cust_info ' ;
Insert Into silver.crm_cust_info (
     cst_id	,
     cst_key ,
     cst_firstname  ,
     cst_lastname  ,
     cst_marital_status  ,
     cst_gndr  ,
     cst_create_date)
Select
cst_id , 
cst_key ,
TRIM (cst_firstname) AS cst_firstname ,
TRIM (cst_lastname) AS cst_lastname ,
Case When UPPER(TRIM(cst_marital_status)) = 'S' Then 'Single'
     When UPPER(TRIM(cst_marital_status)) = 'M' Then 'Married'
     Else 'n/a'
END cst_marital_status,
Case When UPPER(TRIM(cst_gndr)) = 'F' Then 'Female'
     When UPPER(TRIM(cst_gndr)) = 'M' Then 'Male'
     Else 'n/a'
END cst_gndr,
cst_create_date 
from (
Select * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_last
from bronze.crm_cust_info ) t
Where flag_last = 1 ;

-- ReChecking the quality of data entered in silver table

select cst_id , COUNT(*) from silver.crm_cust_info 
GROUP BY cst_id 
Having COUNT(*) > 1 OR cst_id is NUll ;

-- Check for unwanted Spaces 
-- Expectation : No Results 

Select cst_firstname 
From silver.crm_cust_info
Where cst_firstname != Trim(cst_firstname) ;

Select cst_lastname 
From silver.crm_cust_info
Where cst_lastname != Trim(cst_lastname) ;

Select cst_gndr 
From silver.crm_cust_info
Where cst_gndr != Trim(cst_gndr) ;

-- Data Standarization & Consistency 
Select Distinct cst_gndr 
From silver.crm_cust_info ;

Select Distinct cst_marital_status 
From silver.crm_cust_info ;

select * from silver.crm_cust_info ;

-- Data Cleaning , processing and standardization of bronze.crm_prd_info table
Select 
     prd_id	 ,
     prd_key ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     prd_cost  ,
     prd_line  ,
     prd_start_dt ,
     prd_end_dt from bronze.crm_prd_info  ;

select prd_id , COUNT(*) from silver.crm_prd_info 
GROUP BY prd_id 
Having COUNT(*) > 1 OR prd_id is NUll ;

-- There are no entries that are to be trimmed 
Select prd_nm 
From bronze.crm_prd_info 
Where prd_nm != TRIM(prd_nm) ;

-- Comparing the product Key with ID 
Select 
     prd_id	 ,
     prd_key ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     prd_nm	,
     prd_cost  ,
     prd_line  ,
     prd_start_dt ,
     prd_end_dt from bronze.crm_prd_info 
WHERE REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') NOT In 
(select DISTINCT id from bronze.erp_px_cat_g1v2) ;

-- Comparing the Product Key with sales product key
Select 
     prd_id	 ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     prd_cost  ,
     prd_line  ,
     prd_start_dt ,
     prd_end_dt from bronze.crm_prd_info  
Where SUBSTRING(prd_key , 7 , LEN(prd_key)) NOT IN 
( SELECT sls_prd_key FROM bronze.crm_sales_details ) ;

-- Check for nulls or negative numbers 
-- Expectation : No Results 
Select prd_cost 
From bronze.crm_prd_info 
where prd_cost < 0 OR prd_cost is NULL ;

-- Converting the null values to zeros in product cost column 
Select 
     prd_id	 ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     ISNULL (prd_cost,0) AS prd_cost  ,
     prd_line ,
     prd_start_dt ,
     prd_end_dt
from bronze.crm_prd_info  ;

-- Data Standardization 
Select Distinct prd_line 
From bronze.crm_prd_info ;

-- Standardizing the Data 
Select 
     prd_id	 ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     ISNULL (prd_cost,0) AS prd_cost  ,
     Case WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
          WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
          WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
          WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a' 
     End AS prd_line ,
     prd_start_dt ,
     prd_end_dt
from bronze.crm_prd_info  ;

-- Check for invalid Data Entries 
Select * from bronze.crm_prd_info 
where prd_end_dt < prd_start_dt ;

-- Fixing The Data Entry Issue 
Select prd_id ,
       prd_key ,
       prd_nm,
       prd_start_dt,
       prd_end_dt ,
Lead (prd_start_dt) OVER (Partition BY prd_key ORDER BY prd_start_dt )-1 AS prd_end_dt_test 
from bronze.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509') ;

-- Checking Data in Whole of Table 
-- Removing the time from start and end date by casting it to date 
Select 
     prd_id	 ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     ISNULL (prd_cost,0) AS prd_cost  ,
     Case WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
          WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
          WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
          WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a' 
     End AS prd_line ,
     CAST(prd_start_dt AS DATE ) AS prd_start_dt,
     CAST( Lead (prd_start_dt) OVER (Partition BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt 
from bronze.crm_prd_info  ;

-- Now , before putting data in the silver table , first modify the table according the newly formed columns 
Print '>> Truncating Table : silver.crm_prd_info ' ;
Truncate Table silver.crm_prd_info ;
Print '>> Inserting Data Into : silver.crm_prd_info ' ;
INSERT INTO silver.crm_prd_info (
     prd_id ,
     cat_id ,
     prd_key ,
     prd_nm	,
     prd_cost ,
     prd_line ,
     prd_start_dt ,
     prd_end_dt
)
Select 
     prd_id	 ,
     REPLACE(SUBSTRING(prd_key , 1, 5), '-' , '_') as cat_id,
     SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key ,
     prd_nm	,
     ISNULL (prd_cost,0) AS prd_cost  ,
     Case WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
          WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
          WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
          WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a' 
     End AS prd_line ,
     CAST(prd_start_dt AS DATE ) AS prd_start_dt,
     CAST( Lead (prd_start_dt) OVER (Partition BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info  ;

-- Rechecking the Data After Putting in Silver Tables 
-- Checks for NULLS or Duplicates 
SELECT prd_id , COUNT(*)
from silver.crm_prd_info 
group by prd_id 
Having COUNT(*) > 1 or prd_id is NULL ;

-- Check for unwanted Spaces 
-- Expectation : No Results 
SELECT prd_nm from silver.crm_prd_info 
where prd_nm != TRIM(prd_nm) ;

-- Check for NULLS and negative values 
-- Expectation : NO results 
select prd_cost from silver.crm_prd_info 
where prd_cost < 0 OR prd_cost is NULL ;

-- Data Standardization & Consistency 
Select Distinct prd_line from silver.crm_prd_info ;

-- Check For Invalid Date Orders 
Select * from silver.crm_prd_info 
where prd_end_dt = prd_start_dt ;

select * from silver.crm_prd_info ;

-- Data Cleaning , processing and standardization of bronze.crm_sales_details table
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       sls_order_dt ,
       sls_ship_dt ,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details  ;

-- Checking unwanted spaces 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       sls_order_dt ,
       sls_ship_dt ,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details 
Where sls_ord_num != TRIM(sls_ord_num);

-- Cross-Checking the sales product key with other table 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       sls_order_dt ,
       sls_ship_dt ,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details
Where sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) ;

-- Cross_Checking the customer id key 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       sls_order_dt ,
       sls_ship_dt ,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details
Where sls_cust_id NOT IN (Select cst_id FROM silver.crm_cust_info ) ;

-- Check for invalid dates 
SELECT sls_order_dt 
from bronze.crm_sales_details 
where sls_order_dt <= 0 ;

-- Convert the 0s to Nulls 
SELECT NULLIF( sls_order_dt , 0 ) sls_order_dt 
from bronze.crm_sales_details 
where sls_order_dt <= 0 ;

-- Also Convert to Null if date string value is not equal to 8 
SELECT NULLIF( sls_order_dt , 0 ) sls_order_dt 
from bronze.crm_sales_details 
where sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Check the boundaries of date according to year such as greater than 2030 and less than 1900
SELECT NULLIF( sls_order_dt , 0 ) sls_order_dt 
from bronze.crm_sales_details 
where sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20300112 OR sls_order_dt < 19000612 ;

-- Casting the format of sls_order_dt column to date 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) 
       END AS sls_order_dt,
       sls_ship_dt ,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details;

-- Now , checking the shipping date 
SELECT NULLIF( sls_ship_dt,0) sls_ship_dt 
from bronze.crm_sales_details 
where sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20300112 OR sls_ship_dt < 19000612 ;

-- Changing the format of sls_ship_dt column to date 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) 
       END AS sls_order_dt, 
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE) 
       END AS sls_ship_dt,
       sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details;

-- Checking the due date 
SELECT NULLIF( sls_due_dt, 0) sls_due_dt
from bronze.crm_sales_details 
where sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20300112 OR sls_due_dt < 19000612 ;

-- Changing the format of sls_due_dt column to date 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) 
       END AS sls_order_dt, 
       CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE) 
       END AS sls_ship_dt,
       sls_due_dt,
       CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE) 
       END AS sls_due_dt,
       sls_sales,
       sls_quantity ,
       sls_price 
from bronze.crm_sales_details;

-- Check for invalid Orders 
Select * FROM bronze.crm_sales_details 
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt ;

-- Check Data Consistency : Between Sales , Quantity , and Price 
-- >> Sales = Quantity * Price 
-- >> Values must not be null , zero , or negative 

Select sls_sales , sls_quantity , sls_price FROM bronze.crm_sales_details 
where sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price is NULL 
OR sls_sales <= 0
 OR sls_quantity <= 0
 OR sls_price <= 0 
 order by sls_sales , sls_quantity , sls_price ;
 
 -- Formatting the sls columns
 SELECT DISTINCT 
 sls_sales AS old_sls_sales ,
 sls_quantity ,
 sls_price AS old_sls_price ,
 CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
       THEN sls_quantity * ABS(sls_price)
ELSE sls_sales 
END AS sls_sales ,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity , 0)
     ELSE sls_price 
END AS sls_price 
FROM bronze.crm_sales_details 
where sls_sales != sls_quantity * sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price is NULL 
OR sls_sales <= 0
 OR sls_quantity <= 0
 OR sls_price <= 0 
 order by sls_sales , sls_quantity , sls_price ;

 -- Combining the entire Query 
 Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) 
       END AS sls_order_dt, 
       CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE) 
       END AS sls_ship_dt,
       CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE) 
       END AS sls_due_dt,
       
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
       THEN sls_quantity * ABS(sls_price)
ELSE sls_sales 
END AS sls_sales ,
       sls_quantity ,
       CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity , 0)
     ELSE sls_price 
END AS sls_price  
from bronze.crm_sales_details;

--  Reload the silver.crm_sales_details table after implementing all the modifications that are done on the table 

-- Insert data into silver.crm_sales_details 
Print '>> Truncating Table : silver.crm_sales_details ' ;
Truncate Table silver.crm_sales_details ;
Print '>> Inserting Data Into : silver.crm_sales_details ' ;
INSERT INTO silver.crm_sales_details (
    sls_ord_num	,
    sls_prd_key , 
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price
) 
Select sls_ord_num ,
       sls_prd_key ,
       sls_cust_id ,
       CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE) 
       END AS sls_order_dt, 
       CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE) 
       END AS sls_ship_dt,
       CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE) 
       END AS sls_due_dt,
       
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
       THEN sls_quantity * ABS(sls_price)
ELSE sls_sales 
END AS sls_sales ,
       sls_quantity ,
       CASE WHEN sls_price IS NULL OR sls_price <= 0 
     THEN sls_sales / NULLIF(sls_quantity , 0)
     ELSE sls_price 
END AS sls_price  
from bronze.crm_sales_details;

-- Cross Checking the data in silver.crm_sales_details  

-- Check for invalid Data Entries
SELECT * from silver.crm_sales_details 
Where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt ;

select * from silver.crm_sales_details ;

-- Data Cleaning , processing and standardization of bronze.erp_px_cat_g1v2 
Select cid , bdate , gen from bronze.erp_cust_az12 ;

-- Formatting the cid column and removing nas at the start
Select 
CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END AS cid ,
bdate , gen
from bronze.erp_cust_az12 ;

-- Matching with cst_key of silver.crm_cust_info table 
Select 
CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END AS cid ,
bdate , gen
from bronze.erp_cust_az12 
Where CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END NOT In (select DISTINCT cst_key From Silver.crm_cust_info) ;

-- Identify out of range dates 
Select Distinct bdate 
from bronze.erp_cust_az12 
where bdate < '1925-01-01' OR bdate > GETDATE() ;

-- Formatting the bdate column to fix the issue of out of range date 
Select 
CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END AS cid ,
CASE WHEN bdate > GETDATE() THEN NULL 
     ELSE bdate 
END AS bdate ,
gen
from bronze.erp_cust_az12 ;

-- Data Standardization & Consistency 
SELECT DISTINCT gen from bronze.erp_cust_az12 ;

SELECT DISTINCT gen ,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE' ) THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen 
from bronze.erp_cust_az12 ;

-- Combining the query 
Select 
CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END AS cid ,
CASE WHEN bdate > GETDATE() THEN NULL 
     ELSE bdate 
END AS bdate ,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE' ) THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12 ;

-- We didn't change any data type or column name so we would directly insert into the silver.erp_cust_az12 
Print '>> Truncating Table : silver.erp_cust_az12 ' ;
Truncate Table silver.erp_cust_az12 ;
Print '>> Inserting Data Into : silver.erp_cust_az12 ' ;
INSERT INTO silver.erp_cust_az12 (
cid , 
bdate , 
gen )
Select 
CASE When cid LIKE 'NAS%' THEN SUBSTRING (cid , 4 , LEN(cid))
     ELSE cid 
END AS cid ,
CASE WHEN bdate > GETDATE() THEN NULL 
     ELSE bdate 
END AS bdate ,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE' ) THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12 ;

-- Cross Checking the data in silver.erp_cust_az12

-- Identify out of range Dates 
Select Distinct 
bdate from silver.erp_cust_az12 
where bdate < '1925-01-01' OR bdate > GETDATE() ;

-- Data Standardization & Consistently 
Select Distinct gen from silver.erp_cust_az12 ;

select * from silver.erp_cust_az12;

-- Data Cleaning , processing and standardization of bronze.erp_loc_a101 
select cid , cntry from bronze.erp_loc_a101 ;

-- We are joining 2 tables with the help of cid and cst_key so they should match with each other 
select cid , cntry from bronze.erp_loc_a101 ;
select cst_key from silver.crm_cust_info ;

-- We are removing '-' from cid 
Select REPLACE(cid, '-' , '') cid ,
cntry from bronze.erp_loc_a101 ;

-- Rechecking the results 
SELECT REPLACE (cid,'-','') cid , cntry from bronze.erp_loc_a101 
where REPLACE (cid, '-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info ) ;

-- Data Standardization & Consistency 
Select Distinct cntry from bronze.erp_loc_a101 
Order BY cntry ;

SELECT REPLACE (cid,'-','') cid ,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United States '
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
     ELSE TRIM(cntry)
END AS cntry 
FROM bronze.erp_loc_a101 ;

-- Now , we would directly insert data into the silver table becuase we have not changed anything in the table 
Print '>> Truncating Table : silver.erp_loc_a101 ' ;
Truncate Table silver.erp_cust_az12 ;
Print '>> Inserting Data Into : silver.erp_loc_a101 ' ;
INSERT INTO silver.erp_loc_a101 (cid,cntry) 
SELECT REPLACE (cid,'-','') cid ,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United States '
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
     ELSE TRIM(cntry)
END AS cntry 
FROM bronze.erp_loc_a101 ;

-- Cross Checking the data in silver.erp_loc_a101
Select DISTINCT cntry 
FROM silver.erp_loc_a101 
ORDER BY cntry ;

select * from silver.erp_loc_a101 ;

-- Data Cleaning , processing and standardization of bronze.erp_px_cat_g1v2 
SELECT id , cat , subcat , maintenance
from bronze.erp_px_cat_g1v2  ;

-- Check For unwanted Spaces 
SELECT * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) ;

-- Data Standardization & Consistency 
SELECT DISTINCT cat from bronze.erp_px_cat_g1v2 ;

SELECT DISTINCT subcat from bronze.erp_px_cat_g1v2 ;

SELECT DISTINCT maintenance from bronze.erp_px_cat_g1v2 ;

-- The data is clean and standardized 
-- We would directly load it in the silver.erp_px_cat_g1v2 table 

Print '>> Truncating Table : silver.erp_px_cat_g1v2 ' ;
Truncate Table silver.erp_px_cat_g1v2 ;
Print '>> Inserting Data Into : silver.erp_px_cat_g1v2 ' ;
INSERT INTO silver.erp_px_cat_g1v2 (
id , cat , subcat , maintenance )
SELECT 
id , cat , subcat , maintenance
from bronze.erp_px_cat_g1v2  ;

select * from silver.erp_px_cat_g1v2 ;
GO

-- We dont need to cross check the silver table as the data was absolutely clean and no changes were made 
