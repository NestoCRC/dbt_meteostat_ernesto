WITH raw_products AS (
SELECT  *
FROM {{source('northwind_data', 'products')}}
),
quantity_correction AS (
SELECT productid AS product_id,
       productname AS product_name,
       supplierid AS supplier_id,
       categoryid AS category_id,
       quantityperunit AS raw_quantity_label,
       (substring(quantityperunit FROM '[0-9]+' ))::INT AS unit_quantity,
       COALESCE ( 
       (substring(quantityperunit FROM '[0-9]+.*?([0-9]+)'))::INT, 1 
       ) AS measure_amount,
       substring(quantityperunit FROM '[0-9]+.*?[0-9]+\s*([a-zA-Z]+)') AS unit_measure_label,
       unitprice::numeric AS unit_price,
       unitsinstock AS units_in_stock,
       unitsonorder AS units_on_order,
       reorderlevel AS reorder_level,
       discontinued
FROM raw_products
)
SELECT * FROM quantity_correction