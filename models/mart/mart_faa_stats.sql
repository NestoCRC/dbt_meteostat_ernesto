WITH departures AS (
SELECT origin, count(DISTINCT dest) AS nunique_to,
       COUNT(sched_dep_time) AS dep_planned,
       SUM(cancelled) AS dep_cancelled, --You need to sum because the column is interger. 
                                        --Even if you cast as boolean the operation is not feasilble bc SQL doesn't add Booleans
       SUM(diverted) AS dep_diverted,
       COUNT(*) - SUM(cancelled) AS dep_n_flights_calc,
       COUNT(arr_time) AS dep_n_flights
FROM {{ref('prep_flights')}}
group BY origin
),
arrivals AS (
SELECT dest, 
       COUNT(DISTINCT origin) AS nunique_from,
       COUNT(sched_arr_time ) AS arr_planned,
       SUM(cancelled) AS arr_cancelled,
       SUM(diverted) AS arr_diverted,
       COUNT(*) - SUM(cancelled) AS arr_n_flights_calc,
       COUNT(arr_time) AS arr_n_flights
FROM {{ref('prep_flights')}}
group BY dest
),
total_stats AS (
SELECT d.origin AS airport_code,
       d.nunique_to,
       a.nunique_from,
       dep_planned + arr_planned AS total_planned, 
       dep_cancelled + arr_cancelled AS total_cancelled,
       dep_diverted + arr_diverted AS total_diverted, 
       dep_n_flights_calc + arr_n_flights_calc AS total_n_flights_calc,
       dep_n_flights + arr_n_flights AS total_n_flights
FROM departures d
JOIN arrivals a
ON d.origin = a.dest
)
SELECT city, country, name, ts.* 
FROM total_stats ts
JOIN {{ref('prep_airports')}} ap
ON ts.airport_code = ap.faa


