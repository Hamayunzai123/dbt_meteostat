WITH airports_reorder AS (
    SELECT faa                      -- Airport code
           ,name                     -- Airport name
           ,city
           ,region
           ,country
           ,lat                      -- Latitude
           ,lon                      -- Longitude
           ,alt                      -- Altitude
           ,tz                       -- Timezone
           ,dst   
    FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder