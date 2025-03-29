-- Creating Schema for the silver table
drop table silver.crm_prd_info;
Create Table silver.crm_prd_info(
prd_id int,
prd_key varchar(50),
cat_id varchar(50),
prd_name varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt  date,
dwh_created_date DATE DEFAULT CURRENT_DATE
);

drop table silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gender            VARCHAR(50),
    cst_create_date     DATE,
	dwh_created_date DATE DEFAULT CURRENT_DATE
);

drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
	dwh_created_date DATE DEFAULT CURRENT_DATE

);

drop table silver.erp_cust_az12;
create table silver.erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50),
	dwh_created_date DATE DEFAULT CURRENT_DATE

);


drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
cid varchar(50),
country varchar(50),
	dwh_created_date DATE DEFAULT CURRENT_DATE

);


drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
suncat varchar(50),
maintenance varchar(50),
	dwh_created_date DATE DEFAULT CURRENT_DATE

);


