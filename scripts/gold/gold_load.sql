--  dimension customers
create view gold.dim_customers as 
select 
 row_number() over(order by ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as firstname,
ci.cst_lastname as lastname,
la.country,
ci.cst_gender as gender,
case when ci.cst_marital_status = 'M' then 'Married' 
     when ci.cst_marital_status = 'S' then 'Single'
	 else ci.cst_marital_status
	 end as married_status,
	 ca.bdate as birth_date,
ci.cst_create_date as created_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;

--  Product dimension
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_name       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.suncat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data


--  Fact Sales View
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

