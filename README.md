# Ecommerce Sale and Traffic Analysis [SQL in BigQuery]

## About
This project aims to explore key patterns of **Google Merchandise Store**, an *E-commerce* that sells Google-branded merchandise. Utilizing **window functions, time formatting syntax, and multiple CTEs** to clean and extract data to address the business hypotheses.

## Pre-requisite
You need access to a **Google Cloud** project with **BigQuery API** enabled. Complete the *Before you begin* section in the [BigQuery Quickstart guide](https://cloud.google.com/bigquery/docs/quickstarts/query-public-dataset-console#before-you-begin) to create a new Google Cloud project or to enable the BigQuery API in an existing one.

## Using the dataset
The data is sourced from **Google Analytics** and loaded into a table in **BigQuery** to create a database for easy management and analysis. To use the dataset, follow the below steps:
- In the navigation panel of Bigquery, select **Add Data** and then **Search a project**.
- Enter the project ID `bigquery-public-data.google_analytics_sample.ga_sessions` and click **Enter**.
- Click on the `ga_sessions_` table to open it.
- Click [here](https://support.google.com/analytics/answer/3437719?hl=en) to view **Table Schema**.

## Explore data
Learn how to write query and explore *sales* and *traffic* metrics of the E-commerce company.

### 1. Web traffic metrics by specific time
Together with **total pageviews** and **transactions**, **total visits** data gives an overview of a website's reach and helps businesses **track traffic growth and online visibility**. This information helps them determine the total number of interactions with a website.

:black_nib: Calculate **total visit, pageview, transaction** for Jan, Feb and March 2017 (order by month).
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
SELECT 
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  COUNT(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' and '0331'
GROUP BY month
ORDER BY month;
```
</details>
<details>
<summary>Click to expand result!</summary>

#### Result
![image](https://github.com/user-attachments/assets/0029b426-8374-4900-8957-88b6d2cd207a)

From January to March 2017, there was a **slight decrease in total visits and pageviews in February**, but total transactions still increased. These figures were generally rising in **March**, with a **notable 35% increase in total transactions**.
</details>

___

### 2. Bounce rate metric
The **Bounce rate** metric shows the ratio of visits that contained exactly one hit compared to the number of visits entering on that page. 

:black_nib: Bounce rate per traffic source in July 2017, order by total_visit descending order.
<details>
<summary>Click to expand query!</summary>
  
#### Query
```sql
SELECT 
  trafficSource.source,
  COUNT(totals.visits) AS total_visits,
  COUNT(totals.bounces) AS total_no_of_bounces,
  ROUND(COUNT(totals.bounces)*100/COUNT(totals.visits),3) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC;
```
</details>
<details>
<summary>Click to expand result!</summary>
  
#### Result
![image](https://github.com/user-attachments/assets/eb710749-0184-4468-af22-973ab7f4947f)

Although the bounce rate can **vary greatly** and be influenced by a wide range of factors, in general, **high visitation sources tend to have high bounce rates**.
</details>

___

### 3. Revenue by traffic source
Revenue by Traffic Source is a **breakdown of total revenue by traffic source**. This metric **highlights the most valuable sources** that direct traffic to the company's E-commerce site.

:black_nib: Revenue by traffic source by week, by month in June 2017.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
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
```
</details>
<details>
<summary>Click to expand result!</summary>

#### Result
![image](https://github.com/user-attachments/assets/add4599a-91dd-41ce-a6ba-01bc2c0ec6ae)

**Direct** is the source that brings in the **largest revenue** compared to the other sources.
</details>

___

### 4. Average pageviews by purchaser type
Look at the behavior of **purchasers** and n**on-purchasers** to see **how many pageviews they spend on average** before making a purchase decision.

:black_nib: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
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
```
</details>
<details>
<summary>Click to expand result!</summary>

#### Result
![image](https://github.com/user-attachments/assets/913cd7f6-26ea-41f1-a7d3-1dcd28c7509f)

Average pageviews for **non-purchases** are **higher** than those for **purchases**: more than x3 in June and more than x2.5 in July.
</details>

___

### 5. Average transactions per user
Transactions per User is a metric that measures the **average number of transactions made by each individual user on a website**.

:black_nib: Average number of transactions per user that made a purchase in July 2017.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
SELECT 
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  ROUND(SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId),9) AS avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
WHERE totals.transactions >= 1
  AND product.productRevenue IS NOT NULL
GROUP BY month;
```
</details>
<details>
<summary>Click to expand result!</summary>
  
#### Result
![image](https://github.com/user-attachments/assets/347f9bca-7560-4064-9a5c-21253eb51e4b)

The average number of transactions per user who made a purchase in July was **approximately 4**, which was a good figure.
</details>

___

### 6. Average money spent per session
Measure the amount of **revenue generated per user session** on a website.

:black_nib: Average amount of money spent per session. Only include purchaser data in July 2017.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) AS month,
  ROUND(SUM(product.productRevenue/1000000)/COUNT(totals.visits),2) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
WHERE totals.transactions IS NOT NULL
  AND product.productRevenue IS NOT NULL
GROUP BY month;
```
</details>
<details>
<summary>Click to expand result!</summary>

#### Result
![image](https://github.com/user-attachments/assets/0c9110ca-42e2-4ead-9ab9-e4c3b9cddbc8)
</details>

___

### 7. Products purchased by specific customers
**Segmenting customers** based on their **purchase history** is a powerful way for businesses to improve their marketing and customer retention strategies.

:black_nib: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
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
```
</details>
<details>
<summary>Click to expand result!</summary>

  #### Result
![image](https://github.com/user-attachments/assets/2373d46e-6757-4557-87d7-58b815202cf6)

It can be seen that these customers have a great interest in **fashion** items as most of the products they buy are related to clothing.
</details>

___

### 8. Cohort map from product view to addtocart to purchase
**Cohort analysis** groups people with **similar experiences** but at **different times** so that their behavior at similar stages of their life cycles can **be compared**.

:black_nib: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. \
For example, 100% product view then 40% add_to_cart and 10%.
<details>
<summary>Click to expand query!</summary>

#### Query
```sql
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
```
</details>
<details>
<summary>Click to expand result!</summary>
  
#### Result
![image](https://github.com/user-attachments/assets/399fa523-3531-4050-9f4e-38fdfb2d89ed)

January to March 2017 shows **positive signs** as add_to_cart_rate and purchase_rate **both increase gradually**.
</details>
