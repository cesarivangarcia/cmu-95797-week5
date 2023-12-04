#calculating the 7-day moving average of precipitation using the central park weather table
WITH WeatherWithWindow AS (
    SELECT

        date AS date,
        prcp AS prcp,
        AVG(prcp) OVER wnd AS avg_prcp_7d
    FROM
        {{ref("stg__central_park_weather")}}
    WINDOW wnd AS (
        ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    )
)

SELECT
    date,
    prcp,
    avg_prcp_7d
FROM
    WeatherWithWindow