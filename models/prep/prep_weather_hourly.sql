WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'Day') AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN timestamp::time BETWEEN TIME '00:00' AND TIME '05:59' THEN 'night'
            WHEN timestamp::time BETWEEN TIME '06:00' AND TIME '11:59' THEN 'morning'
            WHEN timestamp::time BETWEEN TIME '12:00' AND TIME '17:59' THEN 'afternoon'
            WHEN timestamp::time BETWEEN TIME '18:00' AND TIME '23:59' THEN 'evening'
		END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features



