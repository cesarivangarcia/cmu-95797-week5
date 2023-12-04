#rank events based on the most recent event timestamp for each combination of event_id, event_type, and user_id

WITH RankedEvents AS (
    SELECT
        insert_timestamp,
        event_id,
        event_type,
        user_id,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY event_id, event_type, user_id ORDER BY event_timestamp DESC) AS row_num
    FROM
        {{ref("events")}}
)

#Selecting the most recent event for each combination of event_id, event_type, and user_id

SELECT
    insert_timestamp,
    event_id,
    event_type,
    user_id,
    event_timestamp
FROM
    RankedEvents
where
    row_num = 1