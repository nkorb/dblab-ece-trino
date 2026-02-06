SELECT sum(cs_ext_sales_price) AS sales
FROM mongodb.tpcds.catalog_sales
WHERE cs_sold_date_sk
BETWEEN 2451545 AND 2488070;
