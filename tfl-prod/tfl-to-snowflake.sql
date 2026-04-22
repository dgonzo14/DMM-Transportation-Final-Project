CREATE OR REPLACE FILE FORMAT TFL_JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  IGNORE_UTF8_ERRORS = TRUE;

CREATE OR REPLACE STAGE TFL_R2_STAGE
  URL = 's3compat://tfl-datasync/bronze/tfl/'
  CREDENTIALS = (
    AWS_KEY_ID = '53fd75240250f1c9cf65cc6b068c2e9b'
    AWS_SECRET_KEY = '9a5c1ff6b77f03d43b47083aa5cc1ae55df584330ade60a8eb6f3501d38114b8'
  )

  ENDPOINT = 'ef10a2eac081012eec2538b5a48e0f79.r2.cloudflarestorage.com'
  REGION = 'auto';

LIST @TFL_R2_STAGE;


CREATE OR REPLACE TABLE TFL_ARRIVALS_RAW (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING
);

CREATE OR REPLACE TABLE TFL_STATUS_RAW (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING
);

-- Table for Lift/Elevator accessibility data
CREATE OR REPLACE TABLE TFL_LIFT_DISRUPTIONS_RAW (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING
);

-- Table for Station Crowding metrics
CREATE OR REPLACE TABLE TFL_CROWDING_RAW (
    raw_data VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING
);

COPY INTO TFL_ARRIVALS_RAW (raw_data, file_name)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @TFL_R2_STAGE/arrivals/
)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO TFL_STATUS_RAW (raw_data, file_name)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @TFL_R2_STAGE/status/
)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

-- Port Lift Disruptions (Array-based JSON)
COPY INTO TFL_LIFT_DISRUPTIONS_RAW (raw_data, file_name)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @TFL_R2_STAGE/lift_disruptions/
)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

-- Port Crowding Data
-- Note: Crowding files are single objects, so we override STRIP_OUTER_ARRAY
COPY INTO TFL_CROWDING_RAW (raw_data, file_name)
FROM (
  SELECT $1, METADATA$FILENAME
  FROM @TFL_R2_STAGE/crowding/
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';

---------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SILVER_TFL_ARRIVALS AS
SELECT
    raw_data:id::STRING AS prediction_id,
    raw_data:vehicleId::STRING AS vehicle_id,
    raw_data:naptanId::STRING AS station_id,
    raw_data:stationName::STRING AS station_name,
    raw_data:lineId::STRING AS line_id,
    raw_data:platformName::STRING AS platform,
    raw_data:direction::STRING AS direction,
    raw_data:timeToStation::INT AS seconds_to_arrival,
    TO_TIMESTAMP_NTZ(raw_data:expectedArrival::STRING) AS expected_arrival_ts,
    TO_TIMESTAMP_NTZ(raw_data:timing:read::STRING) AS data_captured_at,
    file_name
FROM TFL_ARRIVALS_RAW;

CREATE OR REPLACE VIEW SILVER_TFL_LINE_STATUS AS
SELECT
    raw_data:id::STRING AS line_id,
    raw_data:name::STRING AS line_name,
    s.value:statusSeverity::INT AS severity_code,
    s.value:statusSeverityDescription::STRING AS status_description,
    s.value:reason::STRING AS disruption_reason,
    ingested_at AS record_timestamp
FROM TFL_STATUS_RAW,
LATERAL FLATTEN(input => raw_data:lineStatuses) s;

CREATE OR REPLACE VIEW SILVER_TFL_LIFT_DISRUPTIONS AS
SELECT
    raw_data:stationUniqueId::STRING AS station_id,
    l.value::STRING AS lift_id,
    raw_data:message::STRING AS full_message,
    ingested_at AS record_timestamp
FROM TFL_LIFT_DISRUPTIONS_RAW,
LATERAL FLATTEN(input => raw_data:disruptedLiftUniqueIds) l;

CREATE OR REPLACE VIEW SILVER_TFL_CROWDING AS
SELECT
    -- Extracts the ID between 'crowding_' and '.json' at the end of the string
    REGEXP_SUBSTR(file_name, 'crowding_([A-Z0-9]+)\.json', 1, 1, 'e', 1) AS naptan_id,
    raw_data:dataAvailable::BOOLEAN AS is_live,
    raw_data:percentageOfBaseline::FLOAT AS crowding_percentage,
    TO_TIMESTAMP_NTZ(raw_data:timeUtc::STRING) AS observation_time_utc,
    file_name
FROM TFL_CROWDING_RAW;

CREATE OR REPLACE VIEW GOLD_TFL_ARRIVAL_PERFORMANCE AS
WITH trip_lifecycle AS (
    SELECT
        PREDICTION_ID,
        STATION_ID,
        STATION_NAME,
        VEHICLE_ID,
        LINE_ID,
        FIRST_VALUE(EXPECTED_ARRIVAL_TS) OVER (PARTITION BY PREDICTION_ID ORDER BY DATA_CAPTURED_AT ASC) AS initial_expected_ts,
        LAST_VALUE(EXPECTED_ARRIVAL_TS) OVER (PARTITION BY PREDICTION_ID ORDER BY DATA_CAPTURED_AT ASC) AS actual_arrival_ts,
        MIN(DATA_CAPTURED_AT) OVER (PARTITION BY PREDICTION_ID) AS first_poll_ts,
        MAX(DATA_CAPTURED_AT) OVER (PARTITION BY PREDICTION_ID) AS last_poll_ts,
        SECONDS_TO_ARRIVAL
    FROM SILVER_TFL_ARRIVALS
)
SELECT
    PREDICTION_ID,
    STATION_ID,
    STATION_NAME,
    VEHICLE_ID,
    LINE_ID,
    initial_expected_ts,
    actual_arrival_ts,
    DATEDIFF('second', initial_expected_ts, actual_arrival_ts) AS prediction_error_seconds,
    DATEDIFF('minute', first_poll_ts, last_poll_ts) AS minutes_tracked,
    STDDEV(SECONDS_TO_ARRIVAL) OVER (PARTITION BY PREDICTION_ID) AS prediction_jitter
FROM trip_lifecycle
WHERE
    -- Applying your data quality filters here
    (minutes_tracked > 0 OR SECONDS_TO_ARRIVAL < 30)
    AND minutes_tracked < 90
    AND ABS(DATEDIFF('second', initial_expected_ts, actual_arrival_ts)) < 600
QUALIFY ROW_NUMBER() OVER (PARTITION BY PREDICTION_ID ORDER BY last_poll_ts DESC) = 1;



SELECT
    LINE_ID,
    COUNT(*) as total_trips,
    COUNT_IF(minutes_tracked < 2 AND prediction_error_seconds > 60) as ghost_train_candidates,
    ROUND(AVG(minutes_tracked), 2) as avg_visibility_mins
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
GROUP BY 1
ORDER BY ghost_train_candidates DESC;

SELECT
    LINE_ID,
    STATION_NAME,
    ROUND(AVG(prediction_error_seconds), 2) as mean_absolute_error,
    ROUND(STDDEV(prediction_error_seconds), 2) as error_volatility,
    COUNT(PREDICTION_ID) as sample_size
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
GROUP BY 1, 2
HAVING sample_size > 10
ORDER BY mean_absolute_error DESC;

SELECT
    LINE_ID,
    CASE
        WHEN HOUR(initial_expected_ts) BETWEEN 7 AND 9 THEN 'AM Peak'
        WHEN HOUR(initial_expected_ts) BETWEEN 16 AND 18 THEN 'PM Peak'
        ELSE 'Off-Peak'
    END as period,
    AVG(prediction_error_seconds) as avg_bias_seconds,
    STDDEV(prediction_error_seconds) as variability
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT
    STATION_NAME,
    LINE_ID,
    AVG(prediction_jitter) as avg_jitter
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
WHERE initial_expected_ts IS NOT NULL
GROUP BY 1, 2
ORDER BY avg_jitter DESC
LIMIT 20;

ALTER STAGE TFL_R2_STAGE SET DIRECTORY = (ENABLE = TRUE);


CREATE OR REPLACE TASK TFL_AUTO_INGEST_TASK
  WAREHOUSE = 'GORILLA_WH' -- Double check your warehouse name
  SCHEDULE = '10 MINUTE'
AS
EXECUTE IMMEDIATE $$
BEGIN
    -- 1. Refresh the stage directory to see new files
    ALTER STAGE TFL_R2_STAGE REFRESH;

    -- 2. Ingest new Arrivals
    COPY INTO TFL_ARRIVALS_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/arrivals/)
    FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT');

    -- 3. Ingest new Line Status
    COPY INTO TFL_STATUS_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/status/)
    FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT');

    -- 4. Ingest new Crowding (using your specific single-object format)
    COPY INTO TFL_CROWDING_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/crowding/)
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE);
END;
$$;

-- IMPORTANT: Tasks are created in a 'SUSPENDED' state. You must start it!
ALTER TASK TFL_AUTO_INGEST_TASK RESUME;

ALTER TASK TFL_AUTO_INGEST_TASK SUSPEND;

-- See if the task is scheduled or running
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TFL_AUTO_INGEST_TASK'
))
ORDER BY SCHEDULED_TIME DESC;

SELECT
    'Arrivals' as TABLE_NAME, COUNT(*) as ROWS_ADDED, MAX(ingested_at) as LAST_LOAD
    FROM TFL_ARRIVALS_RAW WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
UNION ALL
SELECT
    'Status' as TABLE_NAME, COUNT(*) as ROWS_ADDED, MAX(ingested_at) as LAST_LOAD
    FROM TFL_STATUS_RAW WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
UNION ALL
SELECT
    'Crowding' as TABLE_NAME, COUNT(*) as ROWS_ADDED, MAX(ingested_at) as LAST_LOAD
    FROM TFL_CROWDING_RAW WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP());


SELECT COUNT(*) FROM SILVER_TFL_ARRIVALS
WHERE DATA_CAPTURED_AT > DATEADD('hour', -1, CURRENT_TIMESTAMP());