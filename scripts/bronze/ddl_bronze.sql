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



