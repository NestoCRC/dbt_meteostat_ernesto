WITH staging__orders AS(
SELECT * 
FROM {{source('northwind','orders')}}
),
SELECT orderid::varchar AS order_id,
       customerid AS customer_id,
       employeeid AS employee_id,
       orderdate::DATE AS order_date,
       requireddate::DATE AS required_date,
       shippeddate::DATE AS shipped_date,
       shipname AS customer_name,
       shipaddress AS address,
       shipcity AS city,
       shipregion AS region, 
       shippostalcode AS postal_code,
       shipcountry AS country
FROM staging__orders