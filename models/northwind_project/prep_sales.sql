WITH prep_products AS (
SELECT *  
FROM {{ref('staging__products')}}
),
prep_order_details AS (
SELECT * 
FROM {{ref('staging__order_details')}}
),
prep_orders AS (
SELECT * 
FROM {{ref('staging__orders')}}
),
prep_categories AS (
SELECT *
FROM {{ref('staging__categories')}}    
),
joined AS (
SELECT o.order_id,
       o.customer_id,
       o.customer_name,
       date_part('month', o.order_date) AS order_month,
       date_part('year', o.order_date)::varchar AS order_year,
       p.unit_quantity,
       p.unit_price,
       od.discount,
       (p.unit_price * p.unit_quantity * (1 - od.discount)) AS revenue,
       c.category_name
FROM prep_orders o
JOIN prep_order_details od ON o.order_id = od.order_id
JOIN prep_products p ON od.product_id = p.product_id
JOIN prep_categories c ON c.category_id = p.category_id
)
SELECT * FROM joined