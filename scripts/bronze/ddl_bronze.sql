-- Creating a Table for all the files that has to be loaded

Create Table Bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_name varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt  date
);

drop table Bronze.crm_sales_details;
create table Bronze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int

);


create table Bronze.erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50)

);


create table Bronze.erp_loc_a101(
cid varchar(50),
country varchar(50)

);



create table Bronze.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
suncat varchar(50),
maintenance varchar(50)

);



-- Loading the data fro csv Files to sql server using copy function 
-- Data base use is postgreSql
Create or replace  procedure Bronze.load_bronze()
language plpgsql
as $$
BEGIN
 Raise NOTICE 'Loading Bronze Layer';
truncate table Bronze.crm_cust_info;
copy Bronze.crm_cust_info
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
delimiter ','
csv Header;


truncate table Bronze.crm_prd_info;
copy Bronze.crm_prd_info
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
delimiter ','
csv Header;


truncate table Bronze.crm_sales_details;
copy Bronze.crm_sales_details
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
delimiter ','
csv Header;



truncate table Bronze.erp_cust_az12;
copy Bronze.erp_cust_az12
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
delimiter ','
csv Header;



truncate table Bronze.erp_loc_a101;
copy Bronze.erp_loc_a101
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
delimiter ','
csv Header;


truncate table Bronze.erp_px_cat_g1v2;
copy Bronze.erp_px_cat_g1v2
from 'D:\Projects\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
delimiter ','
csv Header;

END $$;



-- Calling the above sored procedure to load the data
call Bronze.load_bronze()



