WITH category_raw AS (
SELECT *
FROM {{source('northwind_data', 'categories')}}
)
Select categoryid AS category_id,
       categoryname AS category_name,
       description
FROM category_raw
