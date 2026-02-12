SELECT 
    p.origin,
    ao.country AS o_country,
    ao.city AS o_city,
    ao.name AS o_name,
    p.dest AS destination,
    ad.country AS d_country,
    ad.city AS d_city,
    ad.name d_name,
    COUNT(*) AS total_flights,
    COUNT(DISTINCT tail_number) AS unique_airplanes,
    COUNT(DISTINCT airline) AS unique_airlines, 
    AVG(actual_elapsed_time_interval) AS avg_elapsed_time,
    AVG(dep_delay_interval) AS avg_dep_delay,
    AVG(arr_delay_interval) AS avg_arr_delay,
    MAX(arr_delay_interval) AS max_arr_delay,
    MIN(arr_delay_interval) AS min_arr_delay,
    SUM(cancelled) AS total_cancelled,
    SUM(diverted) AS total_diverted
FROM  {{ref('prep_flights')}} p
JOIN {{ref('prep_airports')}} ao
ON ao.faa = p.origin
JOIN {{ref('prep_airports')}} ad
ON ad.faa = p.dest
GROUP BY origin, dest, ao.country,ao.city,ao.name,ad.country,ad.city,ad.name

