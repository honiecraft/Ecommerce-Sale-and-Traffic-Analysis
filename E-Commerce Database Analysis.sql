
--QUERY 01----
SELECT 
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  COUNT(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' and '0331'
GROUP BY month
ORDER BY month;

----QUERY 02----
SELECT 
  trafficSource.source,
  COUNT(totals.visits) AS total_visits,
  COUNT(totals.bounces) AS total_no_of_bounces,
  ROUND(COUNT(totals.bounces)*100/COUNT(totals.visits),3) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

----QUERY 03----
SELECT
  'Week' AS time_type,
  FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time,
  trafficSource.source AS source,
  ROUND(SUM(product.productRevenue)/1000000,4) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST (hits) AS hits,
  UNNEST (hits.product) AS product
WHERE product.productRevenue IS NOT NULL
GROUP BY 2, 3

UNION ALL

SELECT
  'Month' AS time_type,
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  trafficSource.source AS source,
  ROUND(SUM(product.productRevenue)/1000000,4) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST (hits) AS hits,
  UNNEST (hits.product) AS product
WHERE product.productRevenue IS NOT NULL
GROUP BY 2, 3
ORDER BY revenue DESC;

----QUERY 04----
WITH formatted_tbl_cte AS (
  SELECT 
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
    fullVisitorId,
    totals.pageviews,
    CASE WHEN totals.transactions >=1 AND productRevenue IS NOT NULL THEN 1
         WHEN totals.transactions IS NULL AND productRevenue IS NULL THEN 0
         END AS purchaser_type
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
  WHERE _table_suffix BETWEEN '0601' and '0731'
),

cal_avg_cte AS (
  SELECT 
    month,
    purchaser_type, 
    ROUND(SUM(pageviews)/COUNT(DISTINCT fullVisitorId),8) AS avg_pageview
  FROM formatted_tbl_cte
  GROUP BY 1,2
  HAVING purchaser_type IS NOT NULL
)

SELECT
  month,
  MAX(CASE WHEN purchaser_type = 1 THEN avg_pageview END) AS avg_pageviews_purchase,
  MAX(CASE WHEN purchaser_type = 0 THEN avg_pageview END) AS avg_pageviews_non_purchase
FROM cal_avg_cte
GROUP BY month
ORDER BY month;

----QUERY 05----
SELECT 
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  ROUND(SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId),9) AS avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
WHERE totals.transactions >= 1
  AND product.productRevenue IS NOT NULL
GROUP BY month;

----QUERY 06----
-- Expected output 43.85 <> (Result 43.8566 ROUND(2) = 43.86)
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  ROUND(SUM(product.productRevenue/1000000)/COUNT(totals.visits),2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
WHERE totals.transactions IS NOT NULL
  AND product.productRevenue IS NOT NULL
GROUP BY month
;

----QUERY 07----
WITH distinct_user AS (
  SELECT
    DISTINCT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
  WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL
)

SELECT DISTINCT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) AS hits,
  UNNEST (hits.product) AS product
WHERE product.v2ProductName <> "YouTube Men's Vintage Henley"
  AND product.productRevenue IS NOT NULL
  AND fullVisitorId IN (SELECT fullVisitorId FROM distinct_user)
GROUP BY product.v2ProductName
ORDER BY quantity DESC;

----QUERY 08----
WITH cal_num_prod_cte AS(
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
    COUNTIF(hits.eCommerceAction.action_type = '2') AS num_product_view,
    COUNTIF(hits.eCommerceAction.action_type = '3') AS num_addtocart,
    COUNTIF(hits.eCommerceAction.action_type = '6'
      AND product.productRevenue IS NOT NULL) AS num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
  WHERE _table_suffix BETWEEN '0101' and '0331'
  GROUP BY month
  ORDER BY month
)

SELECT
  *,
  ROUND(100*num_addtocart/num_product_view,2) AS add_to_cart_rate,
  ROUND(100*num_purchase/num_product_view,2) AS purchase_rate
FROM cal_num_prod_cte;