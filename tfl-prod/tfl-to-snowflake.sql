-- =============================================================================
-- TfL Data Pipeline - Schema v2
-- Cleaned up + MTA-ready structure
-- =============================================================================

CREATE OR REPLACE FILE FORMAT TFL_JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  IGNORE_UTF8_ERRORS = TRUE;


ALTER STAGE TFL_R2_STAGE SET DIRECTORY = (ENABLE = TRUE);

-- =============================================================================
-- BRONZE: Raw ingestion tables
-- =============================================================================

CREATE OR REPLACE TABLE TFL_ARRIVALS_RAW (
    raw_data     VARIANT,
    ingested_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name    STRING
);

CREATE OR REPLACE TABLE TFL_STATUS_RAW (
    raw_data     VARIANT,
    ingested_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name    STRING
);

CREATE OR REPLACE TABLE TFL_LIFT_DISRUPTIONS_RAW (
    raw_data     VARIANT,
    ingested_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name    STRING
);

CREATE OR REPLACE TABLE TFL_CROWDING_RAW (
    raw_data     VARIANT,
    ingested_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name    STRING
);

-- =============================================================================
-- Initial load
-- =============================================================================

COPY INTO TFL_ARRIVALS_RAW (raw_data, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/arrivals/)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO TFL_STATUS_RAW (raw_data, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/status/)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

COPY INTO TFL_LIFT_DISRUPTIONS_RAW (raw_data, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/lift_disruptions/)
FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT')
ON_ERROR = 'CONTINUE';

-- Crowding files are single objects, not arrays
COPY INTO TFL_CROWDING_RAW (raw_data, file_name)
FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/crowding/)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- SILVER: Typed, flattened views
-- =============================================================================

CREATE OR REPLACE VIEW SILVER_TFL_ARRIVALS AS
SELECT
    raw_data:id::STRING                              AS prediction_id,
    raw_data:vehicleId::STRING                       AS vehicle_id,
    raw_data:naptanId::STRING                        AS station_id,
    raw_data:stationName::STRING                     AS station_name,
    raw_data:lineId::STRING                          AS line_id,
    raw_data:platformName::STRING                    AS platform,
    raw_data:direction::STRING                       AS direction,
    raw_data:timeToStation::INT                      AS seconds_to_arrival,
    TO_TIMESTAMP_NTZ(raw_data:expectedArrival::STRING) AS expected_arrival_ts,
    TO_TIMESTAMP_NTZ(raw_data:timing:read::STRING)   AS data_captured_at,
    file_name
FROM TFL_ARRIVALS_RAW;

CREATE OR REPLACE VIEW SILVER_TFL_LINE_STATUS AS
SELECT
    raw_data:id::STRING                          AS line_id,
    raw_data:name::STRING                        AS line_name,
    s.value:statusSeverity::INT                  AS severity_code,
    s.value:statusSeverityDescription::STRING    AS status_description,
    s.value:reason::STRING                       AS disruption_reason,
    ingested_at                                  AS record_timestamp
FROM TFL_STATUS_RAW,
LATERAL FLATTEN(input => raw_data:lineStatuses) s;

CREATE OR REPLACE VIEW SILVER_TFL_LIFT_DISRUPTIONS AS
SELECT
    raw_data:stationUniqueId::STRING             AS station_id,
    l.value::STRING                              AS lift_id,
    raw_data:message::STRING                     AS full_message,
    ingested_at                                  AS record_timestamp
FROM TFL_LIFT_DISRUPTIONS_RAW,
LATERAL FLATTEN(input => raw_data:disruptedLiftUniqueIds) l;

CREATE OR REPLACE VIEW SILVER_TFL_CROWDING AS
SELECT
    -- Tolerates path prefixes changing; anchors on 'crowding_' token
    REGEXP_SUBSTR(file_name, 'crowding_([A-Z0-9]+)\\.json$', 1, 1, 'e', 1) AS naptan_id,
    raw_data:dataAvailable::BOOLEAN              AS is_live,
    raw_data:percentageOfBaseline::FLOAT         AS crowding_percentage,
    TO_TIMESTAMP_NTZ(raw_data:timeUtc::STRING)   AS observation_time_utc,
    ingested_at,
    file_name
FROM TFL_CROWDING_RAW;

-- =============================================================================
-- GOLD: Arrival performance
-- Fixed: LAST_VALUE frame, alias references moved to outer CTE
-- Prep: 'agency' column added now (static 'TfL') so MTA rows slot in cleanly
-- =============================================================================

CREATE OR REPLACE VIEW GOLD_TFL_ARRIVAL_PERFORMANCE AS
WITH raw_lifecycle AS (
    SELECT
        prediction_id,
        station_id,
        station_name,
        vehicle_id,
        line_id,
        seconds_to_arrival,
        data_captured_at,
        expected_arrival_ts,
        -- Correct frames: both need UNBOUNDED FOLLOWING to see the full partition
        FIRST_VALUE(expected_arrival_ts) OVER (
            PARTITION BY prediction_id
            ORDER BY data_captured_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS initial_expected_ts,
        LAST_VALUE(expected_arrival_ts) OVER (
            PARTITION BY prediction_id
            ORDER BY data_captured_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS final_expected_ts,
        MIN(data_captured_at) OVER (PARTITION BY prediction_id) AS first_poll_ts,
        MAX(data_captured_at) OVER (PARTITION BY prediction_id) AS last_poll_ts,
        STDDEV(seconds_to_arrival) OVER (PARTITION BY prediction_id) AS prediction_jitter
    FROM SILVER_TFL_ARRIVALS
),
deduplicated AS (
    SELECT DISTINCT
        'TfL'                                                           AS agency,
        prediction_id,
        station_id,
        station_name,
        vehicle_id,
        line_id,
        initial_expected_ts,
        final_expected_ts                                               AS actual_arrival_ts,
        DATEDIFF('second', initial_expected_ts, final_expected_ts)      AS prediction_error_seconds,
        DATEDIFF('minute', first_poll_ts, last_poll_ts)                 AS minutes_tracked,
        prediction_jitter,
        first_poll_ts,
        last_poll_ts,
        CASE
            WHEN HOUR(initial_expected_ts) BETWEEN 7  AND 9  THEN 'AM Peak'
            WHEN HOUR(initial_expected_ts) BETWEEN 16 AND 18 THEN 'PM Peak'
            ELSE 'Off-Peak'
        END AS service_period,
        -- On-time flag: |error| <= 30s
        CASE WHEN ABS(DATEDIFF('second', initial_expected_ts, final_expected_ts)) <= 30
             THEN 1 ELSE 0 END AS is_on_time
    FROM raw_lifecycle
    -- Filter in outer CTE so aliases resolve cleanly
    WHERE 1=1
)
SELECT *
FROM deduplicated
WHERE
    (minutes_tracked > 0 OR prediction_error_seconds < 30)
    AND minutes_tracked < 90
    AND ABS(prediction_error_seconds) < 600
QUALIFY ROW_NUMBER() OVER (PARTITION BY prediction_id ORDER BY last_poll_ts DESC) = 1;

-- =============================================================================
-- Scheduled auto-ingest task
-- =============================================================================

CREATE OR REPLACE TASK TFL_AUTO_INGEST_TASK
  WAREHOUSE = 'GORILLA_WH'
  SCHEDULE  = '10 MINUTE'
AS
EXECUTE IMMEDIATE $$
BEGIN
    ALTER STAGE TFL_R2_STAGE REFRESH;

    COPY INTO TFL_ARRIVALS_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/arrivals/)
    FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT');

    COPY INTO TFL_STATUS_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/status/)
    FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT');

    COPY INTO TFL_LIFT_DISRUPTIONS_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/lift_disruptions/)
    FILE_FORMAT = (FORMAT_NAME = 'TFL_JSON_FORMAT');

    COPY INTO TFL_CROWDING_RAW (raw_data, file_name)
    FROM (SELECT $1, METADATA$FILENAME FROM @TFL_R2_STAGE/crowding/)
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE);
END;
$$;

ALTER TASK TFL_AUTO_INGEST_TASK RESUME;

-- =============================================================================
-- Monitoring queries
-- =============================================================================

-- Task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TFL_AUTO_INGEST_TASK'))
ORDER BY SCHEDULED_TIME DESC;

-- Row counts for last hour
SELECT 'Arrivals' AS tbl, COUNT(*) AS rows_added, MAX(ingested_at) AS last_load
    FROM TFL_ARRIVALS_RAW  WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'Status',           COUNT(*), MAX(ingested_at)
    FROM TFL_STATUS_RAW    WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'Crowding',         COUNT(*), MAX(ingested_at)
    FROM TFL_CROWDING_RAW  WHERE ingested_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP());

-- Ghost trains by line
SELECT
    line_id,
    COUNT(*) AS total_trips,
    COUNT_IF(minutes_tracked < 2 AND prediction_error_seconds > 60) AS ghost_train_count,
    ROUND(ghost_train_count / NULLIF(total_trips, 0) * 100, 2) AS ghost_train_pct,
    ROUND(AVG(minutes_tracked), 2) AS avg_visibility_mins
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
GROUP BY 1
ORDER BY ghost_train_pct DESC;

-- On-time % by line + period
SELECT
    line_id,
    service_period,
    ROUND(AVG(is_on_time) * 100, 1) AS on_time_pct,
    COUNT(*) AS sample_size
FROM GOLD_TFL_ARRIVAL_PERFORMANCE
GROUP BY 1, 2
HAVING sample_size > 10
ORDER BY 1, 2;