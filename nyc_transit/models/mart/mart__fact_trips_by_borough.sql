with borough as 
(
   select 
            loc.Borough as Borough 
            from {{ ref("mart__fact_all_taxi_trips") }}  all_trip join {{ ref("mart__dim_locations") }} loc
            on all_trip.pulocationid =  loc.locationid  

)
select * from borough

