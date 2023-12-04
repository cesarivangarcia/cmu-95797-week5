# gather all data 
with all_taxi_data as 
(
select 
            tt.pickup_datetime,
            tt.dropoff_datetime,
            tt.duration_sec,
            tt.duration_min,
            tt.pulocationid,
            tt.dolocationid,
            lt.LocationID AS pickup_location_id,
            lt.Borough AS pickup_borough,
            lt.Zone AS pickup_zone,
            lt.service_zone AS pickup_service_zone
        from {{ ref("mart__fact_all_taxi_trips") }} tt 
        join {{ ref("mart__dim_locations") }} lt
        on tt.pulocationid =  lt.locationid     

)

#calculate lead time for the next pickup by zone
 ,TripDataWithLead AS (
    SELECT
        pickup_datetime,
        pickup_zone,
        LEAD(pickup_datetime) OVER (PARTITION BY pickup_zone ORDER BY pickup_datetime) AS next_pickup_datetime
    FROM
        all_taxi_data
),

#calculate time difference between consecutive pickups by zone
Time_difference as 
(
    SELECT
    pickup_zone,
    CAST(datediff('minute', pickup_datetime, next_pickup_datetime) AS DOUBLE PRECISION) AS time_between_pickups
FROM
    TripDataWithLead
WHERE
    next_pickup_datetime IS NOT NULL

)

#calculate average time between pickups by zone
SELECT
    pickup_zone,
    AVG(time_between_pickups) AS avg_time_between_pickups
FROM
    Time_difference
GROUP BY
    pickup_zone