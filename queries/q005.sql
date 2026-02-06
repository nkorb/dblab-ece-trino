select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from psql.tpcds.store_sales, psql.tpcds.date_dim, psql.tpcds.customer
       where psql.tpcds.store_sales.ss_sold_date_sk = psql.tpcds.date_dim.d_date_sk
         and psql.tpcds.store_sales.ss_customer_sk = psql.tpcds.customer.c_customer_sk
         and d_month_seq between 1193 and 1193+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from mongodb.tpcds.catalog_sales, psql.tpcds.date_dim, psql.tpcds.customer
       where mongodb.tpcds.catalog_sales.cs_sold_date_sk = psql.tpcds.date_dim.d_date_sk
         and mongodb.tpcds.catalog_sales.cs_bill_customer_sk = psql.tpcds.customer.c_customer_sk
         and d_month_seq between 1193 and 1193+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from elastic.default.tpcds_web_sales, psql.tpcds.date_dim, psql.tpcds.customer
       where elastic.default.tpcds_web_sales.ws_sold_date_sk = psql.tpcds.date_dim.d_date_sk
         and elastic.default.tpcds_web_sales.ws_bill_customer_sk = psql.tpcds.customer.c_customer_sk
         and d_month_seq between 1193 and 1193+11)
) cool_cust
;
