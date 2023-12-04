#calculate the number of trips and average duration by aggregating taxi trip data and zone locations
select 
        loc.Borough, loc.Zone, count(*) as trip_count, avg(duration_min) as average_trip_duration_min
        from {{ ref("mart__fact_all_taxi_trips") }} all_trip 
         join {{ ref("mart__dim_locations") }} loc
        on all_trip.pulocationid =  loc.locationid  
        group by loc.Borough, loc.Zone     
      