#select zones with less than 100000 trips 
select 
        loc.Zone, count(*) as trip_count
        from {{ ref("mart__fact_all_taxi_trips") }} all_trips 
        join {{ ref("mart__dim_locations") }} loc
        on all_trips.pulocationid =  loc.locationid  
        group by loc.Zone     
        having count(*) < 1000000