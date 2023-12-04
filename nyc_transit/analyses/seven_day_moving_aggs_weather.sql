#calculate the 7-day moving min, max, avg, and sum for precipitation and snow for every day in the central park weather table, defining the window only once

WITH WeatherWith7dayWindow AS (
    SELECT

        date,
        prcp,
        snow,
        AVG(prcp) OVER wnd AS avg_prcp_7d,
        MIN(prcp) OVER wnd AS min_prcp_7d,
        MAX(prcp) OVER wnd AS max_prcp_7d,
        SUM(prcp) OVER wnd AS sum_prcp_7d,
        AVG(snow) OVER wnd AS avg_snow_7d,
        MIN(snow) OVER wnd AS min_snow_7d,
        MAX(snow) OVER wnd AS max_snow_7d,
        SUM(snow) OVER wnd AS sum_snow_7d
        
    FROM
        {{ref("stg__central_park_weather")}}
    WINDOW wnd AS (
        ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    )
)

SELECT
    date,
    prcp,
    snow,
    avg_prcp_7d,
    min_prcp_7d,
    max_prcp_7d,
    sum_prcp_7d,
    avg_snow_7d,
    min_snow_7d,
    max_snow_7d,
    sum_snow_7d
FROM
    WeatherWith7dayWindow