WITH mart_sales AS (
SELECT * 
FROM {{ref('prep_sales')}}
)
SELECT order_year,
       TO_CHAR(TO_DATE(order_month::text, 'MM'), 'FMMonth'),
       category_name,
       count(DISTINCT order_id) AS total_number_of_orders,
       SUM(revenue) AS total_revenue,
       ROUND(avg(revenue),2) AS average_revenue
FROM mart_sales
GROUP BY order_year, order_month,category_name
ORDER BY category_name, order_year, order_month