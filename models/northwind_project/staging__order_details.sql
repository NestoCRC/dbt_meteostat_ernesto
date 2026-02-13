WITH staging_order_details AS (
SELECT *
FROM {{source('northwind_data' , 'order_details')}}
)
SELECT orderid::VARCHAR AS order_id,
       productid AS product_id,
       unitprice::NUMERIC AS unit_price,
       quantity,
       discount::NUMERIC
FROM staging_order_details