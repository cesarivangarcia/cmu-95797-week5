#count taxi trips without a location_id by using a left join with the location table
select 
        count(*) as trips_with_location_null
        from {{ ref("mart__fact_all_taxi_trips") }} taxi_trip 
        Left join {{ ref("mart__dim_locations") }} loc
        on taxi_trip.pulocationid =  loc.locationid  
        where  loc.locationid is null  