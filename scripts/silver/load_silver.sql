call silver.silver_load();

-- DROP PROCEDURE IF EXISTS silver.load_silver;


-- SELECT proname, nspname 
-- FROM pg_proc p
-- JOIN pg_namespace n ON p.pronamespace = n.oid
-- WHERE proname = 'load_silver';


CREATE or REPLACE Procedure  silver.load_silver()
LANGUAGE plpgsql
AS $$
-- Transforming crm_cust_info
-- Checking duplicate Values
-- Removing White Spaces
-- Once cleaning is done we'll be inserting this data into sliver layer same customer info table
truncate table silver.crm_cust_info;
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gender,
cst_create_date
)

select cst_id, 
cst_key,
trim(cst_firstname) as cst_firstname,
trim (cst_lastname) as cst_lastname,
cst_marital_status,
cst_gender,
cst_create_date
from (

select * ,
row_number() over ( partition by cst_id order by cst_create_date desc) as rownum

from Bronze.crm_cust_info
) t  where rownum =1  ; -- selecting only the recent entries and removing Duplicates

-- Loading silver.erp_cust_az12
--  Since we are connectig this table with Customer info, we match check Whether the Ids are matching or not, and transform Accordingly
truncate table silver.erp_cust_az12 ;
Insert into silver.erp_cust_az12(
cid,
bdate,
gen
)

select 
 case when cid like 'NAS%' then substring(cid, 4, length(cid::TEXT)) 
 else cid end as New_cid,
 
 CASE WHEN bdate > current_date then null
 else bdate end as bdate,

 case when lower(gen) in ( 'f', 'female') then 'F'
      when lower(gen) in ( 'm', 'male') then 'M'       
      Else null end as gen
from bronze.erp_cust_az12;


-- Transforming crm_prd_info 
truncate table silver.crm_prd_info;
Insert into silver.crm_prd_info(
prd_id,
prd_key, 
cat_id,
prd_name,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

select
prd_id,
SUBSTRING(prd_key, 7, LENgth(prd_key)) AS prd_key,  
-- here we are taking the sustring and replacing '-' with '_' to use it with other tables
replace ( substring( prd_key, 1, 5), '-', '_') as cat_id, 
prd_name,
-- Replacing  null values with 0
coalesce (prd_cost, 0) as prd_cost ,
prd_line,
prd_start_dt,
-- here there are some issues in the start and end date so will be creating a new col that will have start date from the next entry as a end date for current row
-- using lead () window function to get he next entry for current row
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info;


-- Loading Sales table
truncate table silver.crm_sales_details
Insert into silver.crm_sales_details(
sls_ord_num ,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_due_dt ,
sls_ship_dt ,
sls_sales ,
sls_quantity ,
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or length(sls_order_dt::TEXT) < 8  then null
     else to_date(CAST(sls_order_dt as TEXT), 'YYYYMMDD') 
end as sls_order_dt,

case when sls_due_dt = 0 or length(sls_due_dt::TEXT) < 8  then null
     else to_date(CAST(sls_due_dt as TEXT), 'YYYYMMDD') 
end as sls_due_dt,

case when sls_ship_dt = 0 or length(sls_ship_dt::TEXT) < 8  then null
     else to_date(CAST(sls_ship_dt as TEXT), 'YYYYMMDD') 
end as sls_ship_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;


-- Loading Silver.erp_loc_a101
-- Performed basic cleaning
truncate table silver.erp_loc_a101 ;
insert into silver.erp_loc_a101(
cid, country
)

select 
 replace(cid, '-', '') as cid,
 case when country in ('US','USA') then 'United States'
      when country = 'DE' then 'Germany'
	  when country in ('' ,' ', null) then 'n/a'
	  else country  end as country

from bronze.erp_loc_a101;

truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(id,
cat,
suncat,
maintenance)
select * from bronze.erp_px_cat_g1v2;
 $$;
