-- De-duplicated Silver View for Weather
CREATE OR REPLACE VIEW TRANSIT_WEATHER_DB.CLEAN.WEATHER_HOURLY_FINAL AS
WITH flattened_data AS (
    SELECT 
        COALESCE(RAW_JSON:city::string, 
            CASE 
                WHEN RAW_JSON:latitude::float BETWEEN 40.7 AND 40.8 THEN 'NYC'
                WHEN RAW_JSON:latitude::float BETWEEN 51.4 AND 51.6 THEN 'London'
            END) AS city,
        t.value::string AS time_utc,
        RAW_JSON:hourly:temperature_2m[t.index]::float AS temp_c,
        RAW_JSON:hourly:precipitation[t.index]::float AS precip_mm,
        RAW_JSON:hourly:visibility[t.index]::float AS visibility_m,
        RAW_JSON:hourly:wind_speed_10m[t.index]::float AS wind_speed_kph,
        ROW_NUMBER() OVER (
            PARTITION BY city, t.value::string 
            ORDER BY RAW_JSON:generationtime_ms::float DESC
        ) as row_num
    FROM TRANSIT_WEATHER_DB.RAW.WEATHER_HISTORY,
    LATERAL FLATTEN(input => RAW_JSON:hourly:time) t
)
SELECT city, time_utc, temp_c, precip_mm, visibility_m, wind_speed_kph
FROM flattened_data
WHERE row_num = 1;