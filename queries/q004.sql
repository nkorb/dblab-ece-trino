SELECT
  ss_customer_sk,
  sum(act_sales) AS sumsales
FROM (
  SELECT
    ss.ss_item_sk,
    ss.ss_ticket_number,
    ss.ss_customer_sk,
    CASE
      WHEN sr.sr_return_quantity IS NOT NULL
        THEN (ss.ss_quantity - sr.sr_return_quantity) * ss.ss_sales_price
      ELSE ss.ss_quantity * ss.ss_sales_price
    END AS act_sales
  FROM psql.tpcds.store_sales ss
  LEFT JOIN psql.tpcds.store_returns sr
    ON sr.sr_item_sk = ss.ss_item_sk
   AND sr.sr_ticket_number = ss.ss_ticket_number
  JOIN psql.tpcds.reason r
    ON sr.sr_reason_sk = r.r_reason_sk
) t
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100;
