SELECT sum(ss_ext_sales_price)
AS sales
FROM psql.tpcds.store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2488070;
