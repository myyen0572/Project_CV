-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT left (date,6) AS month    --format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
      ,sum(totals.visits)AS visits
      ,sum(totals.pageviews) AS pageviews
      ,sum(totals.totalTransactionRevenue/1000000) AS revenue,sum(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month   
ORDER BY month   

-- Query 02: Bounce rate per traffic source in July 2017    
#standardSQL
WITH a AS (
      SELECT trafficSource.source AS source
            ,COUNT(totals.visits) AS total_visits
            ,COUNT (totals.bounces) AS num_bounces
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
      GROUP BY trafficSource.source)

SELECT source
      ,total_visits
      ,num_bounces
      ,(num_bounces/a.total_visits*100) AS bounces_rate
FROM a
GROUP BY source,total_visits,num_bounces
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
WITH a AS (
SELECT trafficSource.source AS source, date, sum(totals.totalTransactionRevenue/1000000) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source, date),

b AS (
SELECT format_date("%Y%W", parse_date("%Y%m%d", date)) AS time, source, sum(revenue) AS revenue
FROM a
GROUP BY source, date
ORDER BY date),

week AS (
SELECT  source, sum(revenue) AS revenue,time
FROM b
GROUP BY source, time), 

month AS (
SELECT source, left(date,6) as time, sum(revenue) as revenue
FROM a
GROUP BY source, left(date,6)),

c AS (
SELECT time, source, revenue
FROM month 
UNION ALL
SELECT time, source, revenue
FROM week 
GROUP BY time, source, revenue)

SELECT CASE WHEN RIGHT(time,2) = '06' THEN 'month' ELSE 'week' END as time_type, time, source, revenue
FROM c
ORDER BY revenue DESC

--nếu mình ghi cte a,b,c như vậy nhìn vào sẽ k nắm đc là phần đó mình đang lấy data gì, hong biết nó xử lý qua từng step như thế nào
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
WITH a AS (
SELECT fullvisitorId AS user_id,date, 
(CASE WHEN totals.transactions >= 1 THEN 1 ELSE 0 END) AS buy
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`),

b AS (
SELECT DISTINCT user_id AS user_id, LEFT(date,6) AS date
FROM a
WHERE buy = 1),

num_user AS (
SELECT COUNT(user_id)AS count_user, date
FROM b
GROUP BY date),

c AS (
SELECT totals.transactions AS trans, fullvisitorId AS user_id
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`),

d AS (
  SELECT b.user_id, c.trans
  FROM b
  LEFT JOIN c ON b.user_id = c.user_id),

e AS (
SELECT user_id, SUM(trans) AS sum
FROM d
GROUP BY user_id),

trans AS (
SELECT SUM(sum) AS sumtrans , date
FROM e
FULL JOIN b ON e.user_id =b.user_id
GROUP BY date)

SELECT trans.date, (sumtrans/count_user) AS Avg_total_transactions_per_user
FROM trans
FULL JOIN num_user ON trans.date = num_user.date

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month


-- Query 06: Average amount of money spent per session
#standardSQL
WITH a AS (
SELECT LEFT(date,6) date ,   --format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
       sum(totals.totalTransactionRevenue) AS revenue, 
       sum(totals.visits) AS visits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY date)

SELECT date,
      (revenue/visits) AS avg_revenue_by_user_per_visit
FROM a

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month


