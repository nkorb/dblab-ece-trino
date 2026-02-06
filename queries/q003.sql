SELECT ca.ca_state, count(*) AS orders
FROM mongodb.tpcds.catalog_sales cs
JOIN psql.tpcds.customer_address ca
ON cs.cs_bill_addr_sk = ca.ca_address_sk
WHERE cs.cs_sold_date_sk
BETWEEN 2451545 AND 2488070
GROUP BY ca.ca_state
ORDER BY orders DESC LIMIT 20;
