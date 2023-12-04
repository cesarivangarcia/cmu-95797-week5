#calcuate the average fare amount by borough, zone, and overall using the avg() window function and over clause
select 
        distinct
                      Zone,
                      Borough,
                    AVG(fare_amount) Over (Partition BY Borough)  AS borough_avg_fare,
                    AVG(fare_amount) Over (Partition BY Zone)  AS zone_avg_fare,
                    AVG(fare_amount) Over () as overall_fare
                     from {{ ref("stg__yellow_tripdata") }} yellow_trips 
                          join {{ ref("mart__dim_locations") }} loc
                         on yellow_trips.pulocationid =  loc.locationid  
 