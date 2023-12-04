with pivot_cte as (
      select
      {{ dbt_utils.pivot(
            'Borough',
            dbt_utils.get_column_values(ref("mart__fact_trips_by_borough"), 'Borough')
      ) }}
      from {{ ref("mart__fact_trips_by_borough") }}
)

select * from pivot_cte 