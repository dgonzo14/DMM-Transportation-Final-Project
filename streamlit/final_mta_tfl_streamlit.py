import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide", page_title="Metro Performance Command Center")

session = get_active_session()

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

MTA_DATABASE = "GORILLA_DB"
MTA_SCHEMA = "GORILLA_SCHEMA"
MTA_WAREHOUSE = "GORILLA_WH"

TFL_DATABASE = "TRANSIT_WEATHER_DB"
TFL_SCHEMA = "CLEAN"

MTA_PREFIX = f"{MTA_DATABASE}.{MTA_SCHEMA}."
TFL_PREFIX = f"{TFL_DATABASE}.{TFL_SCHEMA}."


def mta_qname(table_name: str) -> str:
    return f"{MTA_PREFIX}{table_name}"


def tfl_qname(table_name: str) -> str:
    return f"{TFL_PREFIX}{table_name}"


def table_exists(table_name: str) -> bool:
    try:
        session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
        return True
    except Exception:
        return False


def fmt_metric(value, decimals: int = 0, suffix: str = "") -> str:
    if value is None or pd.isna(value):
        return "—"
    if decimals == 0:
        return f"{int(round(float(value))):,}{suffix}"
    return f"{float(value):,.{decimals}f}{suffix}"


def safe_pct(value) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):.1f}%"


def safe_float(value):
    if value is None or pd.isna(value):
        return None
    return float(value)


def add_norm_score(series: pd.Series, high_is_bad: bool = True) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float)
    s = series.astype(float)
    if s.nunique(dropna=True) <= 1:
        return pd.Series([0.0] * len(s), index=s.index)
    ranks = s.rank(pct=True, na_option="bottom")
    return ranks if high_is_bad else (1 - ranks)


def latest_scalar(df: pd.DataFrame, col: str):
    if df.empty or col not in df.columns:
        return None
    val = df.iloc[0][col]
    return None if pd.isna(val) else val


def summarize_network(
    name: str, freshness_s, impacted_rate_pct, punctuality_pct, extra_note: str = ""
) -> tuple[str, str]:
    if impacted_rate_pct is None:
        impacted_rate_pct = 0
    if punctuality_pct is None:
        punctuality_pct = 0

    if impacted_rate_pct >= 50 or punctuality_pct < 70:
        status = "Under Pressure"
    elif impacted_rate_pct >= 25 or punctuality_pct < 80:
        status = "Watch"
    else:
        status = "Stable"

    summary = (
        f"{name}: {status}. "
        f"Impacted service rate {safe_pct(impacted_rate_pct)}, "
        f"punctuality {safe_pct(punctuality_pct)}."
    )
    if extra_note:
        summary += f" {extra_note}"
    return status, summary


def make_dumbbell_chart(df: pd.DataFrame, left_label: str, right_label: str) -> go.Figure:
    fig = go.Figure()
    ordered = df.copy().sort_values("SortValue", ascending=True).reset_index(drop=True)
    metrics = ordered["Metric"].tolist()

    for _, row in ordered.iterrows():
        fig.add_trace(
            go.Scatter(
                x=[row[left_label], row[right_label]],
                y=[row["Metric"], row["Metric"]],
                mode="lines",
                line=dict(width=3, color="rgba(120, 200, 120, 0.7)"),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=ordered[left_label],
            y=ordered["Metric"],
            mode="markers",
            name=left_label,
            marker=dict(size=12),
            hovertemplate=f"<b>{left_label}</b><br>%{{y}}: %{{x:.1f}}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=ordered[right_label],
            y=ordered["Metric"],
            mode="markers",
            name=right_label,
            marker=dict(size=12),
            hovertemplate=f"<b>{right_label}</b><br>%{{y}}: %{{x:.1f}}<extra></extra>",
        )
    )

    for _, row in ordered.iterrows():
        diff = abs(row[left_label] - row[right_label])
        left_yshift = 10 if diff < 3 else 0
        right_yshift = -10 if diff < 3 else 0

        fig.add_annotation(
            x=row[left_label], y=row["Metric"],
            text=f"{row[left_label]:.1f}", showarrow=False,
            xshift=-18, yshift=left_yshift, xanchor="right", font=dict(size=12),
        )
        fig.add_annotation(
            x=row[right_label], y=row["Metric"],
            text=f"{row[right_label]:.1f}", showarrow=False,
            xshift=18, yshift=right_yshift, xanchor="left", font=dict(size=12),
        )

    xmin = min(ordered[left_label].min(), ordered[right_label].min())
    xmax = max(ordered[left_label].max(), ordered[right_label].max())
    pad = (xmax - xmin) * 0.12 if xmax > xmin else 5

    fig.update_layout(
        title_text="",
        margin=dict(t=20, b=20, l=40, r=40),
        xaxis_title="Value",
        yaxis_title="",
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="left", x=0),
        height=420,
    )
    fig.update_xaxes(range=[xmin - pad, xmax + pad])
    fig.update_yaxes(categoryorder="array", categoryarray=metrics)
    return fig


# -----------------------------------------------------------------------------
# Required / optional tables
# -----------------------------------------------------------------------------

# TfL CLEAN schema table names (matching File 1 conventions)
REQUIRED_MTA = [
    "MTA_GOLD_LOAD_HISTORY",
    "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD_V2",
    "MTA_ROUTE_HEADWAYS_GOLD_V2",
    "MTA_ROUTE_ALERT_ACTIVITY_GOLD_V2",
    "MTA_CURRENT_STOP_BOARD_GOLD",
    "MTA_ROUTE_SERVICE_GOLD",
    "MTA_NETWORK_SUMMARY_GOLD",
]

REQUIRED_TFL = [
    "TFL_ARRIVALS_SILVER",
    "TFL_ARRIVAL_PERFORMANCE_GOLD",
    "TFL_STATUS_SILVER",
    "TFL_CROWDING_SILVER",
    "TFL_LIFT_DISRUPTIONS_SILVER",
]

OPTIONAL_MTA = {
    "punctuality": mta_qname("MTA_PUNCTUALITY_COVERAGE_GOLD"),
    "vehicle_positions": mta_qname("MTA_VEHICLE_POSITIONS_SILVER"),
    "service_status_view": mta_qname("MTA_ROUTE_SERVICE_STATUS_V"),
    "stops_dim": mta_qname("MTA_STOPS_DIM"),
}


@st.cache_data(ttl=600)
def check_tables():
    missing_mta = [mta_qname(t) for t in REQUIRED_MTA if not table_exists(mta_qname(t))]
    missing_tfl = [tfl_qname(t) for t in REQUIRED_TFL if not table_exists(tfl_qname(t))]
    optional = {k: table_exists(v) for k, v in OPTIONAL_MTA.items()}
    return missing_mta, missing_tfl, optional


missing_mta, missing_tfl, optional_tables = check_tables()

if missing_mta or missing_tfl:
    st.error("This app can connect to Snowflake, but one or more expected tables are missing or inaccessible.")
    if missing_mta:
        st.write("Missing MTA tables:")
        st.code("\n".join(missing_mta))
    if missing_tfl:
        st.write("Missing TfL tables:")
        st.code("\n".join(missing_tfl))
    st.info(
        "For MTA, confirm the Gorilla gold tables were created and your role has SELECT access. "
        f"For TfL, confirm the tables exist in {TFL_DATABASE}.{TFL_SCHEMA} and your role can query them."
    )
    st.stop()


st.title("🚇 Metro Performance Command Center")
st.caption(
    "Operations-first view of MTA and TfL line performance with a stronger comparison tab, "
    "cross-network watchlist, and city-specific deep dives."
)

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_mta_route_direction_options() -> tuple[list[str], list[str]]:
    route_df = session.sql(
        f"""
        SELECT DISTINCT ROUTE_ID
        FROM {mta_qname('MTA_ROUTE_SERVICE_GOLD')}
        WHERE ROUTE_ID IS NOT NULL
        ORDER BY 1
        """
    ).to_pandas()

    direction_df = session.sql(
        f"""
        SELECT DISTINCT DIRECTION
        FROM {mta_qname('MTA_ROUTE_SERVICE_GOLD')}
        WHERE DIRECTION IS NOT NULL
        ORDER BY 1
        """
    ).to_pandas()

    return (
        ["All routes"] + route_df["ROUTE_ID"].dropna().astype(str).tolist(),
        ["All directions"] + direction_df["DIRECTION"].dropna().astype(str).tolist(),
    )


@st.cache_data(ttl=600)
def load_tfl_line_options() -> list[str]:
    df = session.sql(
        f"""
        SELECT DISTINCT LINE_ID
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE LINE_ID IS NOT NULL
        ORDER BY 1
        """
    ).to_pandas()
    return ["All lines"] + df["LINE_ID"].dropna().astype(str).tolist()


mta_route_options, mta_direction_options = load_mta_route_direction_options()
tfl_line_options = load_tfl_line_options()

with st.sidebar:
    st.header("Controls")
    lookback_hours = st.slider("Trend lookback window (hours)", 2, 72, 24)
    top_n_services = st.slider("Services to show in watchlists/charts", 5, 25, 12)
    stop_board_limit = st.slider("MTA stop board row limit", 20, 200, 75, step=5)

    st.subheader("MTA detail filters")
    selected_mta_route = st.selectbox("MTA route", mta_route_options, index=0)
    selected_mta_direction = st.selectbox("MTA direction", mta_direction_options, index=0)

    st.subheader("TfL detail filters")
    selected_tfl_line = st.selectbox("TfL line", tfl_line_options, index=0)

    st.caption("Comparison + watchlist tabs stay network-wide.")
    st.caption(f"MTA target: {MTA_DATABASE}.{MTA_SCHEMA} on {MTA_WAREHOUSE}")
    st.caption(f"TfL target: {TFL_DATABASE}.{TFL_SCHEMA}")


# -----------------------------------------------------------------------------
# TfL network loaders
# Column names follow TFL_ARRIVALS_SILVER / TFL_ARRIVAL_PERFORMANCE_GOLD /
# TFL_STATUS_SILVER / TFL_CROWDING_SILVER / TFL_LIFT_DISRUPTIONS_SILVER
# as defined in the CLEAN schema (matching File 1 query conventions).
# -----------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_tfl_network_data(lookback: int):
    fresh = session.sql(f"""
        SELECT DATEDIFF('second', MAX(DATA_CAPTURED_AT), CURRENT_TIMESTAMP()) AS LATENCY_SECONDS
        FROM {tfl_qname('TFL_ARRIVALS_SILVER')}
    """).to_pandas()

    network_counts = session.sql(f"""
        SELECT
            COUNT(DISTINCT LINE_ID)       AS ACTIVE_SERVICES,
            COUNT(DISTINCT STATION_ID)    AS ACTIVE_LOCATIONS,
            COUNT(DISTINCT VEHICLE_ID)    AS ACTIVE_TRAINS,
            COUNT(DISTINCT PREDICTION_ID) AS TRACKED_ARRIVALS
        FROM {tfl_qname('TFL_ARRIVALS_SILVER')}
        WHERE DATA_CAPTURED_AT >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
    """).to_pandas()

    latest_status_summary = session.sql(f"""
        WITH latest_per_line AS (
            SELECT
                LINE_ID,
                SEVERITY_CODE,
                RECORD_TIMESTAMP,
                ROW_NUMBER() OVER (
                    PARTITION BY LINE_ID ORDER BY RECORD_TIMESTAMP DESC
                ) AS RN
            FROM {tfl_qname('TFL_STATUS_SILVER')}
        )
        SELECT
            ROUND(AVG(CASE WHEN SEVERITY_CODE = 10 THEN 1 ELSE 0 END) * 100, 1) AS GOOD_SERVICE_PCT,
            COUNT_IF(SEVERITY_CODE <> 10) AS IMPACTED_SERVICES
        FROM latest_per_line
        WHERE RN = 1
    """).to_pandas()

    network_punctuality = session.sql(f"""
        SELECT
            ROUND(AVG(IS_ON_TIME) * 100, 1)                   AS ON_TIME_PCT,
            ROUND(AVG(ABS(PREDICTION_ERROR_SECONDS)), 2)       AS AVG_ABS_ERROR_SEC
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
    """).to_pandas()

    ghost_summary = session.sql(f"""
        SELECT
            ROUND(
                COUNT_IF(MINUTES_TRACKED < 2 AND PREDICTION_ERROR_SECONDS > 60)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS GHOST_RATE_PCT,
            ROUND(AVG(MINUTES_TRACKED), 2) AS AVG_VISIBILITY_MINS
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
    """).to_pandas()

    headway_network = session.sql(f"""
        WITH arrival_times AS (
            SELECT
                LINE_ID, STATION_NAME, ACTUAL_ARRIVAL_TS,
                LAG(ACTUAL_ARRIVAL_TS) OVER (
                    PARTITION BY LINE_ID, STATION_NAME ORDER BY ACTUAL_ARRIVAL_TS
                ) AS PREV_ARRIVAL_TS
            FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
            WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        ),
        gaps AS (
            SELECT LINE_ID,
                DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS) / 60.0 AS HEADWAY_MINS
            FROM arrival_times
            WHERE PREV_ARRIVAL_TS IS NOT NULL
              AND DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS) BETWEEN 60 AND 5400
        )
        SELECT
            ROUND(AVG(HEADWAY_MINS), 2)    AS NETWORK_AVG_HEADWAY_MINS,
            ROUND(MEDIAN(HEADWAY_MINS), 2) AS NETWORK_MEDIAN_HEADWAY_MINS
        FROM gaps
    """).to_pandas()

    service_watchlist = session.sql(f"""
        WITH counts AS (
            SELECT
                LINE_ID AS SERVICE_ID,
                COUNT(DISTINCT STATION_ID)    AS ACTIVE_LOCATIONS,
                COUNT(DISTINCT VEHICLE_ID)    AS ACTIVE_TRAINS,
                COUNT(DISTINCT PREDICTION_ID) AS TRACKED_ARRIVALS
            FROM {tfl_qname('TFL_ARRIVALS_SILVER')}
            WHERE DATA_CAPTURED_AT >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            GROUP BY 1
        ),
        punctuality AS (
            SELECT
                LINE_ID AS SERVICE_ID,
                ROUND(AVG(IS_ON_TIME) * 100, 1)             AS PUNCTUALITY_PCT,
                ROUND(AVG(ABS(PREDICTION_ERROR_SECONDS)), 1) AS AVG_ABS_ERROR_SEC,
                COUNT(*)                                     AS PUNCTUALITY_SAMPLE
            FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
            WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            GROUP BY 1
        ),
        ghosts AS (
            SELECT
                LINE_ID AS SERVICE_ID,
                ROUND(
                    COUNT_IF(MINUTES_TRACKED < 2 AND PREDICTION_ERROR_SECONDS > 60)
                    / NULLIF(COUNT(*), 0) * 100, 2
                ) AS GHOST_RATE_PCT,
                ROUND(AVG(MINUTES_TRACKED), 2) AS AVG_VISIBILITY_MINS
            FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
            WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            GROUP BY 1
        ),
        headway AS (
            WITH arrival_times AS (
                SELECT
                    LINE_ID, STATION_NAME, ACTUAL_ARRIVAL_TS,
                    LAG(ACTUAL_ARRIVAL_TS) OVER (
                        PARTITION BY LINE_ID, STATION_NAME ORDER BY ACTUAL_ARRIVAL_TS
                    ) AS PREV_ARRIVAL_TS
                FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
                WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            )
            SELECT
                LINE_ID AS SERVICE_ID,
                ROUND(AVG(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)) / 60.0, 2)    AS AVG_HEADWAY_MINS,
                ROUND(MEDIAN(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)) / 60.0, 2) AS MEDIAN_HEADWAY_MINS
            FROM arrival_times
            WHERE PREV_ARRIVAL_TS IS NOT NULL
              AND DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS) BETWEEN 60 AND 5400
            GROUP BY 1
        ),
        latest_status AS (
            SELECT
                LINE_ID,
                SEVERITY_CODE,
                RECORD_TIMESTAMP
            FROM (
                SELECT LINE_ID, SEVERITY_CODE, RECORD_TIMESTAMP,
                    ROW_NUMBER() OVER (PARTITION BY LINE_ID ORDER BY RECORD_TIMESTAMP DESC) AS RN
                FROM {tfl_qname('TFL_STATUS_SILVER')}
            )
            WHERE RN = 1
        ),
        universe AS (
            SELECT SERVICE_ID FROM counts
            UNION SELECT SERVICE_ID FROM punctuality
            UNION SELECT SERVICE_ID FROM ghosts
            UNION SELECT SERVICE_ID FROM headway
        )
        SELECT
            'TfL'                                     AS NETWORK,
            u.SERVICE_ID,
            u.SERVICE_ID                              AS SERVICE_LABEL,
            'Line'                                    AS SERVICE_TYPE,
            COALESCE(c.ACTIVE_LOCATIONS, 0)           AS ACTIVE_LOCATIONS,
            COALESCE(c.ACTIVE_TRAINS, 0)              AS ACTIVE_TRAINS,
            COALESCE(c.TRACKED_ARRIVALS, 0)           AS TRACKED_ARRIVALS,
            p.PUNCTUALITY_PCT,
            p.AVG_ABS_ERROR_SEC,
            p.PUNCTUALITY_SAMPLE,
            g.GHOST_RATE_PCT,
            g.AVG_VISIBILITY_MINS,
            h.AVG_HEADWAY_MINS,
            h.MEDIAN_HEADWAY_MINS,
            ls.SEVERITY_CODE,
            CASE WHEN ls.SEVERITY_CODE = 10 OR ls.SEVERITY_CODE IS NULL THEN 0 ELSE 1 END AS IMPACTED_FLAG,
            CASE WHEN ls.SEVERITY_CODE = 10 THEN 'Normal'
                 WHEN ls.SEVERITY_CODE IS NULL THEN 'Unknown'
                 ELSE 'Disrupted' END                 AS STATUS_LABEL,
            100.0                                     AS PUNCTUALITY_COVERAGE_PCT,
            TRUE                                      AS IS_PUNCTUALITY_READY
        FROM universe u
        LEFT JOIN counts       c  ON u.SERVICE_ID = c.SERVICE_ID
        LEFT JOIN punctuality  p  ON u.SERVICE_ID = p.SERVICE_ID
        LEFT JOIN ghosts       g  ON u.SERVICE_ID = g.SERVICE_ID
        LEFT JOIN headway      h  ON u.SERVICE_ID = h.SERVICE_ID
        LEFT JOIN latest_status ls ON UPPER(u.SERVICE_ID) = UPPER(ls.LINE_ID)
    """).to_pandas()

    # TFL_STATUS_SILVER uses LINE_ID (not LINE_NAME) in the CLEAN schema
    trend_impacted = session.sql(f"""
        WITH latest_line_hour AS (
            SELECT
                DATE_TRUNC('hour', RECORD_TIMESTAMP) AS HOUR_BUCKET,
                LINE_ID,
                SEVERITY_CODE,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE_TRUNC('hour', RECORD_TIMESTAMP), LINE_ID
                    ORDER BY RECORD_TIMESTAMP DESC
                ) AS RN
            FROM {tfl_qname('TFL_STATUS_SILVER')}
            WHERE RECORD_TIMESTAMP >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        )
        SELECT
            HOUR_BUCKET,
            ROUND(AVG(CASE WHEN SEVERITY_CODE <> 10 THEN 1 ELSE 0 END) * 100, 2) AS METRIC_VALUE
        FROM latest_line_hour
        WHERE RN = 1
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    trend_headway = session.sql(f"""
        WITH arrival_times AS (
            SELECT
                LINE_ID, STATION_NAME, ACTUAL_ARRIVAL_TS,
                DATE_TRUNC('hour', ACTUAL_ARRIVAL_TS) AS HOUR_BUCKET,
                LAG(ACTUAL_ARRIVAL_TS) OVER (
                    PARTITION BY LINE_ID, STATION_NAME ORDER BY ACTUAL_ARRIVAL_TS
                ) AS PREV_ARRIVAL_TS
            FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
            WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        )
        SELECT
            HOUR_BUCKET,
            ROUND(AVG(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)) / 60.0, 2) AS METRIC_VALUE
        FROM arrival_times
        WHERE PREV_ARRIVAL_TS IS NOT NULL
          AND DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS) BETWEEN 60 AND 5400
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    trend_punctuality = session.sql(f"""
        SELECT
            DATE_TRUNC('hour', ACTUAL_ARRIVAL_TS) AS HOUR_BUCKET,
            ROUND(AVG(IS_ON_TIME) * 100, 2)       AS METRIC_VALUE
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    return {
        "fresh": fresh,
        "network_counts": network_counts,
        "latest_status_summary": latest_status_summary,
        "network_punctuality": network_punctuality,
        "ghost_summary": ghost_summary,
        "headway_network": headway_network,
        "service_watchlist": service_watchlist,
        "trend_impacted": trend_impacted,
        "trend_headway": trend_headway,
        "trend_punctuality": trend_punctuality,
    }


@st.cache_data(ttl=300)
def load_tfl_detail_data(lookback: int, line_filter: str):
    # Build SQL filter fragments; Snowflake parameterised queries not used here
    # because we f-string the lookback int and safely quote the line string.
    line_id_sql = (
        "1=1"
        if line_filter == "All lines"
        else f"UPPER(LINE_ID) = UPPER('{line_filter}')"
    )

    lag_line = session.sql(f"""
        SELECT LINE_ID, ROUND(AVG(PREDICTION_ERROR_SECONDS), 1) AS AVG_ERROR
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        GROUP BY 1 ORDER BY AVG_ERROR DESC LIMIT 1
    """).to_pandas()

    on_time = session.sql(f"""
        SELECT
            LINE_ID, SERVICE_PERIOD,
            ROUND(AVG(IS_ON_TIME) * 100, 1) AS ON_TIME_PCT,
            COUNT(*) AS SAMPLE_SIZE
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        GROUP BY 1, 2
        HAVING SAMPLE_SIZE > 10
        ORDER BY 1, 2
    """).to_pandas()

    # TFL_STATUS_SILVER CLEAN schema uses LINE_ID (not LINE_NAME)
    reliability = session.sql(f"""
        SELECT
            LINE_ID,
            HOUR(RECORD_TIMESTAMP) AS HR,
            AVG(CASE WHEN SEVERITY_CODE = 10 THEN 100 ELSE 0 END) AS RELIABILITY_PCT
        FROM {tfl_qname('TFL_STATUS_SILVER')}
        WHERE RECORD_TIMESTAMP >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        GROUP BY 1, 2 ORDER BY 2
    """).to_pandas()

    ghost_trains = session.sql(f"""
        SELECT
            LINE_ID,
            COUNT(*) AS TOTAL_TRIPS,
            COUNT_IF(MINUTES_TRACKED < 2 AND PREDICTION_ERROR_SECONDS > 60) AS GHOST_COUNT,
            ROUND(
                COUNT_IF(MINUTES_TRACKED < 2 AND PREDICTION_ERROR_SECONDS > 60)
                / NULLIF(COUNT(*), 0) * 100, 2
            ) AS GHOST_PCT,
            ROUND(AVG(MINUTES_TRACKED), 2) AS AVG_VISIBILITY_MINS
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        GROUP BY 1 ORDER BY GHOST_PCT DESC
    """).to_pandas()

    headway = session.sql(f"""
        WITH arrival_times AS (
            SELECT
                LINE_ID, STATION_NAME, ACTUAL_ARRIVAL_TS,
                LAG(ACTUAL_ARRIVAL_TS) OVER (
                    PARTITION BY LINE_ID, STATION_NAME ORDER BY ACTUAL_ARRIVAL_TS
                ) AS PREV_ARRIVAL_TS
            FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
            WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
              AND {line_id_sql}
        )
        SELECT
            LINE_ID,
            ROUND(AVG(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)) / 60.0, 2)    AS AVG_HEADWAY_MINS,
            ROUND(STDDEV(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)) / 60.0, 2) AS HEADWAY_STDDEV,
            ROUND(
                STDDEV(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS))
                / NULLIF(AVG(DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS)), 0) * 100, 1
            ) AS HEADWAY_CV_PCT
        FROM arrival_times
        WHERE PREV_ARRIVAL_TS IS NOT NULL
          AND DATEDIFF('second', PREV_ARRIVAL_TS, ACTUAL_ARRIVAL_TS) BETWEEN 60 AND 5400
        GROUP BY 1 ORDER BY AVG_HEADWAY_MINS DESC
    """).to_pandas()

    gold_sample = session.sql(f"""
        SELECT LINE_ID, PREDICTION_ERROR_SECONDS, SERVICE_PERIOD, IS_ON_TIME
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
    """).to_pandas()

    jitter = session.sql(f"""
        SELECT
            STATION_NAME, LINE_ID,
            ROUND(AVG(PREDICTION_JITTER), 2) AS AVG_JITTER,
            COUNT(*) AS SAMPLE_SIZE
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        GROUP BY 1, 2
        HAVING SAMPLE_SIZE > 5
        ORDER BY AVG_JITTER DESC LIMIT 15
    """).to_pandas()

    # TFL_CROWDING_SILVER: uses OBSERVATION_TIME_UTC and CROWDING_PERCENTAGE
    crowding = session.sql(f"""
        SELECT OBSERVATION_TIME_UTC, CROWDING_PERCENTAGE
        FROM {tfl_qname('TFL_CROWDING_SILVER')}
        WHERE OBSERVATION_TIME_UTC >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        ORDER BY OBSERVATION_TIME_UTC ASC
    """).to_pandas()

    # TFL_LIFT_DISRUPTIONS_SILVER: uses STATION_ID, LIFT_ID, RECORD_TIMESTAMP
    lift = session.sql(f"""
        SELECT STATION_ID, COUNT(DISTINCT LIFT_ID) AS DISRUPTION_COUNT
        FROM {tfl_qname('TFL_LIFT_DISRUPTIONS_SILVER')}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).to_pandas()

    lift_trend = session.sql(f"""
        SELECT
            DATE_TRUNC('hour', RECORD_TIMESTAMP) AS HOUR_BUCKET,
            COUNT(DISTINCT LIFT_ID)              AS DISRUPTIONS
        FROM {tfl_qname('TFL_LIFT_DISRUPTIONS_SILVER')}
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    ticker = session.sql(f"""
        SELECT
            ACTUAL_ARRIVAL_TS::TIME          AS TIME,
            LINE_ID,
            STATION_NAME,
            PREDICTION_ERROR_SECONDS         AS DELAY_SEC,
            IS_ON_TIME
        FROM {tfl_qname('TFL_ARRIVAL_PERFORMANCE_GOLD')}
        WHERE ACTUAL_ARRIVAL_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {line_id_sql}
        ORDER BY ACTUAL_ARRIVAL_TS DESC LIMIT 25
    """).to_pandas()

    return {
        "lag_line": lag_line,
        "on_time": on_time,
        "reliability": reliability,
        "ghost_trains": ghost_trains,
        "headway": headway,
        "gold_sample": gold_sample,
        "jitter": jitter,
        "crowding": crowding,
        "lift": lift,
        "lift_trend": lift_trend,
        "ticker": ticker,
    }


# -----------------------------------------------------------------------------
# MTA loaders (unchanged from original)
# -----------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_mta_network_data(lookback: int):
    network_summary = session.sql(f"""
        SELECT * FROM {mta_qname('MTA_NETWORK_SUMMARY_GOLD')}
        ORDER BY AS_OF_TS DESC LIMIT 1
    """).to_pandas()

    fresh = session.sql(f"""
        SELECT DATEDIFF('second', MAX(LOADED_AT), CURRENT_TIMESTAMP()) AS GOLD_LOAD_LATENCY_SECONDS
        FROM {mta_qname('MTA_GOLD_LOAD_HISTORY')}
        WHERE STATUS = 'SUCCESS'
    """).to_pandas()

    route_service = session.sql(f"""
        SELECT
            ROUTE_ID, DIRECTION, ACTIVE_TRAINS, ACTIVE_VEHICLE_TRIPS,
            FUTURE_STOP_PREDICTIONS, NEXT_ARRIVAL_MIN, AVG_MINUTES_AWAY,
            MEDIAN_MINUTES_AWAY, AVG_HEADWAY_MINS, MIN_HEADWAY_MINS,
            MAX_HEADWAY_MINS, ACTIVE_ALERTS, AVG_TRIP_UPDATES_PER_MIN,
            AVG_VEHICLE_POSITIONS_PER_MIN, AVG_STOP_TIME_UPDATES_PER_MIN, UPDATED_AT
        FROM {mta_qname('MTA_ROUTE_SERVICE_GOLD')}
        ORDER BY ROUTE_ID, DIRECTION
    """).to_pandas()

    route_alerts = session.sql(f"""
        SELECT ROUTE_ID, SUM(ACTIVE_ALERT_COUNT) AS ACTIVE_ALERTS_RECENT
        FROM {mta_qname('MTA_ROUTE_ALERT_ACTIVITY_GOLD_V2')}
        WHERE WINDOW_START >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        GROUP BY 1
    """).to_pandas()

    headway_recent = session.sql(f"""
        SELECT
            ROUTE_ID, DIRECTION,
            DATE_TRUNC('hour', WINDOW_START) AS HOUR_BUCKET,
            ROUND(AVG(AVG_HEADWAY_SECONDS) / 60.0, 2) AS AVG_HEADWAY_MINS
        FROM {mta_qname('MTA_ROUTE_HEADWAYS_GOLD_V2')}
        WHERE WINDOW_START >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        GROUP BY 1, 2, 3 ORDER BY HOUR_BUCKET
    """).to_pandas()

    snapshot_trend = session.sql(f"""
        SELECT
            DATE_TRUNC('hour', SNAPSHOT_TS) AS HOUR_BUCKET,
            COUNT(DISTINCT ROUTE_ID)        AS ACTIVE_SERVICES
        FROM {mta_qname('MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD_V2')}
        WHERE SNAPSHOT_TS >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    alert_trend = session.sql(f"""
        SELECT
            DATE_TRUNC('hour', WINDOW_START) AS HOUR_BUCKET,
            COUNT(DISTINCT CASE WHEN ACTIVE_ALERT_COUNT > 0 THEN ROUTE_ID END) AS IMPACTED_SERVICES,
            SUM(ACTIVE_ALERT_COUNT) AS ACTIVE_ALERTS
        FROM {mta_qname('MTA_ROUTE_ALERT_ACTIVITY_GOLD_V2')}
        WHERE WINDOW_START >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    result = {
        "network_summary": network_summary,
        "fresh": fresh,
        "route_service": route_service,
        "route_alerts": route_alerts,
        "headway_recent": headway_recent,
        "snapshot_trend": snapshot_trend,
        "alert_trend": alert_trend,
    }

    if optional_tables["vehicle_positions"]:
        result["vehicle_counts"] = session.sql(f"""
            SELECT
                ROUTE_ID, DIRECTION,
                COUNT(DISTINCT COALESCE(TRAIN_ID, VEHICLE_ID, TRIP_ID)) AS ACTIVE_TRAINS_RECENT
            FROM {mta_qname('MTA_VEHICLE_POSITIONS_SILVER')}
            WHERE FEED_TIMESTAMP >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
        """).to_pandas()

        result["vehicle_network"] = session.sql(f"""
            SELECT COUNT(DISTINCT COALESCE(TRAIN_ID, VEHICLE_ID, TRIP_ID)) AS ACTIVE_TRAINS_RECENT
            FROM {mta_qname('MTA_VEHICLE_POSITIONS_SILVER')}
            WHERE FEED_TIMESTAMP >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
        """).to_pandas()
    else:
        result["vehicle_counts"] = pd.DataFrame()
        result["vehicle_network"] = pd.DataFrame()

    if optional_tables["punctuality"]:
        result["punctuality_route"] = session.sql(f"""
            SELECT
                ROUTE_ID, DIRECTION,
                ROUND(SUM(ON_TIME_PCT_2MIN * OBSERVED_DELAY_EVENTS) / NULLIF(SUM(OBSERVED_DELAY_EVENTS), 0), 1) AS ON_TIME_PCT_2MIN,
                ROUND(SUM(COVERAGE_PCT * CANDIDATE_EVENTS) / NULLIF(SUM(CANDIDATE_EVENTS), 0), 2) AS COVERAGE_PCT,
                SUM(OBSERVED_DELAY_EVENTS) AS OBSERVED_DELAY_EVENTS,
                MAX(IFF(IS_DASHBOARD_READY, 1, 0)) AS IS_DASHBOARD_READY_NUM
            FROM {mta_qname('MTA_PUNCTUALITY_COVERAGE_GOLD')}
            WHERE GRAIN = 'ROUTE_DIRECTION'
              AND WINDOW_HOUR >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
        """).to_pandas()

        result["punctuality_network"] = session.sql(f"""
            SELECT
                ROUND(SUM(ON_TIME_PCT_2MIN * OBSERVED_DELAY_EVENTS) / NULLIF(SUM(OBSERVED_DELAY_EVENTS), 0), 1) AS NETWORK_ON_TIME_PCT_2MIN,
                ROUND(SUM(COVERAGE_PCT * CANDIDATE_EVENTS) / NULLIF(SUM(CANDIDATE_EVENTS), 0), 2) AS NETWORK_COVERAGE_PCT,
                SUM(OBSERVED_DELAY_EVENTS) AS OBSERVED_DELAY_EVENTS,
                MAX(IFF(IS_DASHBOARD_READY, 1, 0)) AS IS_DASHBOARD_READY_NUM
            FROM {mta_qname('MTA_PUNCTUALITY_COVERAGE_GOLD')}
            WHERE GRAIN = 'NETWORK'
              AND WINDOW_HOUR >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
        """).to_pandas()

        result["punctuality_trend"] = session.sql(f"""
            SELECT
                WINDOW_HOUR AS HOUR_BUCKET,
                ON_TIME_PCT_2MIN AS METRIC_VALUE,
                COVERAGE_PCT
            FROM {mta_qname('MTA_PUNCTUALITY_COVERAGE_GOLD')}
            WHERE GRAIN = 'NETWORK'
              AND WINDOW_HOUR >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
            ORDER BY WINDOW_HOUR
        """).to_pandas()
    else:
        result["punctuality_route"] = pd.DataFrame()
        result["punctuality_network"] = pd.DataFrame()
        result["punctuality_trend"] = pd.DataFrame()

    return result


@st.cache_data(ttl=300)
def load_mta_detail_data(lookback: int, route_filter: str, direction_filter: str, board_limit: int):
    route_sql = "1=1" if route_filter == "All routes" else f"ROUTE_ID = '{route_filter}'"
    direction_sql = "1=1" if direction_filter == "All directions" else f"DIRECTION = '{direction_filter}'"

    route_service = session.sql(f"""
        SELECT
            ROUTE_ID, DIRECTION, ACTIVE_TRAINS, ACTIVE_VEHICLE_TRIPS,
            FUTURE_STOP_PREDICTIONS, NEXT_ARRIVAL_MIN, AVG_MINUTES_AWAY,
            MEDIAN_MINUTES_AWAY, AVG_HEADWAY_MINS, MIN_HEADWAY_MINS,
            MAX_HEADWAY_MINS, ACTIVE_ALERTS, AVG_TRIP_UPDATES_PER_MIN,
            AVG_VEHICLE_POSITIONS_PER_MIN, AVG_STOP_TIME_UPDATES_PER_MIN, UPDATED_AT
        FROM {mta_qname('MTA_ROUTE_SERVICE_GOLD')}
        WHERE {route_sql} AND {direction_sql}
        ORDER BY ROUTE_ID, DIRECTION
    """).to_pandas()

    stop_board = session.sql(f"""
        SELECT
            ROUTE_ID, DIRECTION, STOP_ID, ARRIVAL_RANK, NEXT_EVENT_TS, MINUTES_AWAY,
            COALESCE(TRAIN_ID, '—') AS TRAIN_ID,
            COALESCE(TRACK_DISPLAY, '—') AS TRACK_DISPLAY,
            UPDATED_AT
        FROM {mta_qname('MTA_CURRENT_STOP_BOARD_GOLD')}
        WHERE {route_sql} AND {direction_sql}
          AND NEXT_EVENT_TS >= CURRENT_TIMESTAMP()
          AND NEXT_EVENT_TS <= DATEADD('hour', 2, CURRENT_TIMESTAMP())
        ORDER BY NEXT_EVENT_TS ASC, ROUTE_ID, DIRECTION, STOP_ID, ARRIVAL_RANK
        LIMIT {int(board_limit)}
    """).to_pandas()

    stop_leaderboard = session.sql(f"""
        SELECT
            STOP_ID,
            COUNT(*) AS UPCOMING_TRAINS,
            MIN(MINUTES_AWAY) AS SOONEST_TRAIN_MIN,
            ROUND(AVG(MINUTES_AWAY), 1) AS AVG_WAIT_MIN
        FROM {mta_qname('MTA_CURRENT_STOP_BOARD_GOLD')}
        WHERE {route_sql} AND {direction_sql}
          AND ARRIVAL_RANK <= 3
          AND NEXT_EVENT_TS >= CURRENT_TIMESTAMP()
          AND NEXT_EVENT_TS <= DATEADD('hour', 2, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY UPCOMING_TRAINS DESC, SOONEST_TRAIN_MIN ASC LIMIT 15
    """).to_pandas()

    headway_recent = session.sql(f"""
        SELECT
            ROUTE_ID, DIRECTION,
            ROUND(AVG(AVG_HEADWAY_SECONDS) / 60.0, 2)  AS AVG_HEADWAY_MINS,
            ROUND(AVG(MIN_HEADWAY_SECONDS) / 60.0, 2)  AS MIN_HEADWAY_MINS,
            ROUND(AVG(MAX_HEADWAY_SECONDS) / 60.0, 2)  AS MAX_HEADWAY_MINS,
            SUM(TRIP_COUNT) AS TRIP_COUNT
        FROM {mta_qname('MTA_ROUTE_HEADWAYS_GOLD_V2')}
        WHERE WINDOW_START >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {route_sql} AND {direction_sql}
        GROUP BY 1, 2 ORDER BY AVG_HEADWAY_MINS DESC
    """).to_pandas()

    alert_trend = session.sql(f"""
        SELECT
            DATE_TRUNC('hour', WINDOW_START) AS HOUR_BUCKET,
            SUM(ACTIVE_ALERT_COUNT) AS ACTIVE_ALERTS
        FROM {mta_qname('MTA_ROUTE_ALERT_ACTIVITY_GOLD_V2')}
        WHERE WINDOW_START >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
          AND {route_sql}
        GROUP BY 1 ORDER BY 1
    """).to_pandas()

    result = {
        "route_service": route_service,
        "stop_board": stop_board,
        "stop_leaderboard": stop_leaderboard,
        "headway_recent": headway_recent,
        "alert_trend": alert_trend,
    }

    if optional_tables["punctuality"]:
        result["punctuality_route"] = session.sql(f"""
            SELECT
                ROUTE_ID, DIRECTION,
                ROUND(SUM(ON_TIME_PCT_2MIN * OBSERVED_DELAY_EVENTS) / NULLIF(SUM(OBSERVED_DELAY_EVENTS), 0), 1) AS ON_TIME_PCT_2MIN,
                ROUND(SUM(COVERAGE_PCT * CANDIDATE_EVENTS) / NULLIF(SUM(CANDIDATE_EVENTS), 0), 2) AS COVERAGE_PCT,
                SUM(OBSERVED_DELAY_EVENTS) AS OBSERVED_DELAY_EVENTS
            FROM {mta_qname('MTA_PUNCTUALITY_COVERAGE_GOLD')}
            WHERE GRAIN = 'ROUTE_DIRECTION'
              AND WINDOW_HOUR >= DATEADD('hour', -{lookback}, CURRENT_TIMESTAMP())
              AND {route_sql} AND {direction_sql}
            GROUP BY 1, 2
        """).to_pandas()
    else:
        result["punctuality_route"] = pd.DataFrame()

    return result


# -----------------------------------------------------------------------------
# Load all data
# -----------------------------------------------------------------------------

tfl_network = load_tfl_network_data(lookback_hours)
tfl_detail = load_tfl_detail_data(lookback_hours, selected_tfl_line)
mta_network = load_mta_network_data(lookback_hours)
mta_detail = load_mta_detail_data(lookback_hours, selected_mta_route, selected_mta_direction, stop_board_limit)

# -----------------------------------------------------------------------------
# Derived scalars
# -----------------------------------------------------------------------------

tfl_fresh = tfl_network["fresh"]
tfl_counts = tfl_network["network_counts"]
tfl_status_summary = tfl_network["latest_status_summary"]
tfl_punctuality = tfl_network["network_punctuality"]
tfl_ghost_summary = tfl_network["ghost_summary"]
tfl_headway_network = tfl_network["headway_network"]
tfl_watchlist = tfl_network["service_watchlist"].copy()

mta_summary = mta_network["network_summary"]
mta_fresh = mta_network["fresh"]
mta_watchlist = mta_network["route_service"].copy()
mta_alerts_by_route = mta_network["route_alerts"].copy()
mta_vehicle_counts = mta_network["vehicle_counts"].copy()
mta_vehicle_network = mta_network["vehicle_network"]
mta_punctuality_route = mta_network["punctuality_route"].copy()
mta_punctuality_network = mta_network["punctuality_network"]
mta_punctuality_trend = mta_network["punctuality_trend"].copy()

tfl_fresh_s = safe_float(latest_scalar(tfl_fresh, "LATENCY_SECONDS"))
tfl_active_services = safe_float(latest_scalar(tfl_counts, "ACTIVE_SERVICES"))
tfl_active_locations = safe_float(latest_scalar(tfl_counts, "ACTIVE_LOCATIONS"))
tfl_active_trains = safe_float(latest_scalar(tfl_counts, "ACTIVE_TRAINS"))
tfl_tracked_arrivals = safe_float(latest_scalar(tfl_counts, "TRACKED_ARRIVALS"))
tfl_good_service_pct = safe_float(latest_scalar(tfl_status_summary, "GOOD_SERVICE_PCT"))
tfl_impacted_services = safe_float(latest_scalar(tfl_status_summary, "IMPACTED_SERVICES"))
tfl_punctuality_pct = safe_float(latest_scalar(tfl_punctuality, "ON_TIME_PCT"))
tfl_avg_abs_error = safe_float(latest_scalar(tfl_punctuality, "AVG_ABS_ERROR_SEC"))
tfl_ghost_rate = safe_float(latest_scalar(tfl_ghost_summary, "GHOST_RATE_PCT"))
tfl_avg_headway = safe_float(latest_scalar(tfl_headway_network, "NETWORK_AVG_HEADWAY_MINS"))

mta_source_latency = safe_float(latest_scalar(mta_summary, "SOURCE_LATENCY_SECONDS"))
mta_active_services = safe_float(latest_scalar(mta_summary, "ACTIVE_ROUTES"))
mta_active_locations = safe_float(latest_scalar(mta_summary, "ACTIVE_STOPS_WITH_PREDICTIONS"))
mta_avg_headway = safe_float(latest_scalar(mta_summary, "NETWORK_AVG_HEADWAY_MINS"))
mta_avg_wait = safe_float(latest_scalar(mta_summary, "NETWORK_AVG_WAIT_MINS"))
mta_future_predictions = safe_float(latest_scalar(mta_summary, "FUTURE_STOP_PREDICTIONS"))

mta_active_trains = safe_float(latest_scalar(mta_summary, "ACTIVE_TRAINS"))
if not mta_vehicle_network.empty:
    recent_train_count = safe_float(latest_scalar(mta_vehicle_network, "ACTIVE_TRAINS_RECENT"))
    if recent_train_count is not None:
        mta_active_trains = recent_train_count

mta_punctuality_pct = safe_float(latest_scalar(mta_punctuality_network, "NETWORK_ON_TIME_PCT_2MIN"))
mta_punctuality_coverage = safe_float(latest_scalar(mta_punctuality_network, "NETWORK_COVERAGE_PCT"))
mta_punctuality_ready = (
    bool(latest_scalar(mta_punctuality_network, "IS_DASHBOARD_READY_NUM"))
    if not mta_punctuality_network.empty and "IS_DASHBOARD_READY_NUM" in mta_punctuality_network.columns
    else False
)

mta_punctuality_display = safe_pct(mta_punctuality_pct) if mta_punctuality_pct is not None else "—"
show_mta_active_trains = (
    mta_active_trains is not None
    and pd.notna(mta_active_trains)
    and float(mta_active_trains) > 0
)

if not mta_watchlist.empty:
    if not mta_vehicle_counts.empty:
        mta_watchlist = mta_watchlist.merge(
            mta_vehicle_counts.rename(columns={"ACTIVE_TRAINS_RECENT": "ACTIVE_TRAINS_RECENT_VP"}),
            on=["ROUTE_ID", "DIRECTION"], how="left",
        )
        mta_watchlist["ACTIVE_TRAINS"] = mta_watchlist["ACTIVE_TRAINS_RECENT_VP"].combine_first(mta_watchlist["ACTIVE_TRAINS"])

    if not mta_alerts_by_route.empty:
        mta_watchlist = mta_watchlist.merge(mta_alerts_by_route, on="ROUTE_ID", how="left")
        mta_watchlist["ACTIVE_ALERTS"] = mta_watchlist["ACTIVE_ALERTS_RECENT"].combine_first(mta_watchlist["ACTIVE_ALERTS"])

    if not mta_punctuality_route.empty:
        mta_watchlist = mta_watchlist.merge(
            mta_punctuality_route[["ROUTE_ID", "DIRECTION", "ON_TIME_PCT_2MIN", "COVERAGE_PCT", "OBSERVED_DELAY_EVENTS"]],
            on=["ROUTE_ID", "DIRECTION"], how="left",
        )

    mta_watchlist["SERVICE_LABEL"] = (
        mta_watchlist["ROUTE_ID"].astype(str) + " · " + mta_watchlist["DIRECTION"].fillna("Unknown").astype(str)
    )
    mta_watchlist["NETWORK"] = "MTA"
    mta_watchlist["SERVICE_TYPE"] = "Route / Direction"
    mta_watchlist["PUNCTUALITY_PCT"] = mta_watchlist["ON_TIME_PCT_2MIN"] if "ON_TIME_PCT_2MIN" in mta_watchlist.columns else None
    mta_watchlist["PUNCTUALITY_COVERAGE_PCT"] = mta_watchlist["COVERAGE_PCT"] if "COVERAGE_PCT" in mta_watchlist.columns else None
    mta_watchlist["IMPACTED_FLAG"] = (mta_watchlist["ACTIVE_ALERTS"].fillna(0) > 0).astype(int)

    mta_watchlist["STATUS_LABEL"] = mta_watchlist.apply(
        lambda r: (
            "Severely Disrupted" if (pd.notna(r.get("ACTIVE_ALERTS")) and r["ACTIVE_ALERTS"] > 0 and pd.notna(r.get("ACTIVE_TRAINS")) and r["ACTIVE_TRAINS"] == 0)
            else "Disrupted" if (pd.notna(r.get("ACTIVE_ALERTS")) and r["ACTIVE_ALERTS"] > 0)
            else "Gap Watch" if (pd.notna(r.get("NEXT_ARRIVAL_MIN")) and pd.notna(r.get("AVG_HEADWAY_MINS")) and r["NEXT_ARRIVAL_MIN"] >= max(r["AVG_HEADWAY_MINS"] * 1.5, 10))
            else "Low Visibility" if (pd.notna(r.get("ACTIVE_TRAINS")) and r["ACTIVE_TRAINS"] == 0)
            else "Normal"
        ),
        axis=1,
    )

    mta_watchlist["CONFIDENCE_LABEL"] = mta_watchlist.get(
        "COVERAGE_PCT", pd.Series([None] * len(mta_watchlist))
    ).apply(
        lambda x: "High" if pd.notna(x) and x >= 20 else "Medium" if pd.notna(x) and x >= 10 else "Low" if pd.notna(x) and x >= 5 else "Experimental"
    )

if not tfl_watchlist.empty:
    tfl_watchlist["NETWORK"] = "TfL"
    tfl_watchlist["SERVICE_TYPE"] = "Line"
    tfl_watchlist["CONFIDENCE_LABEL"] = "High"

mta_impacted_services = int(mta_watchlist.loc[mta_watchlist["IMPACTED_FLAG"] == 1, "ROUTE_ID"].nunique()) if not mta_watchlist.empty else None
mta_impacted_rate = (mta_impacted_services / mta_active_services * 100) if mta_active_services not in [None, 0] and mta_impacted_services is not None else None
tfl_impacted_rate = (tfl_impacted_services / tfl_active_services * 100) if tfl_active_services not in [None, 0] and tfl_impacted_services is not None else None

mta_locations_per_service = (mta_active_locations / mta_active_services) if mta_active_services not in [None, 0] and mta_active_locations is not None else None
tfl_locations_per_service = (tfl_active_locations / tfl_active_services) if tfl_active_services not in [None, 0] and tfl_active_locations is not None else None

mta_trains_per_service = (mta_active_trains / mta_active_services) if mta_active_services not in [None, 0] and mta_active_trains is not None else None
tfl_trains_per_service = (tfl_active_trains / tfl_active_services) if tfl_active_services not in [None, 0] and tfl_active_trains is not None else None

mta_status, mta_summary_text = summarize_network(
    "MTA", mta_source_latency, mta_impacted_rate, mta_punctuality_pct,
    "Observed punctuality is coverage-aware.",
)
tfl_status, tfl_summary_text = summarize_network(
    "TfL", tfl_fresh_s, tfl_impacted_rate, tfl_punctuality_pct,
    "Punctuality is directly observed from arrival performance.",
)

takeaway_bits = []
if tfl_impacted_rate is not None and mta_impacted_rate is not None:
    if tfl_impacted_rate > mta_impacted_rate:
        takeaway_bits.append("TfL currently shows the higher impacted service rate.")
    elif mta_impacted_rate > tfl_impacted_rate:
        takeaway_bits.append("MTA currently shows the higher impacted service rate.")

if tfl_avg_headway is not None and mta_avg_headway is not None:
    if tfl_avg_headway > mta_avg_headway:
        takeaway_bits.append("TfL is running with wider average headways.")
    elif mta_avg_headway > tfl_avg_headway:
        takeaway_bits.append("MTA is running with wider average headways.")

if mta_punctuality_ready and tfl_punctuality_pct is not None and mta_punctuality_pct is not None:
    if mta_punctuality_pct > tfl_punctuality_pct:
        takeaway_bits.append("MTA currently has the stronger observed punctuality signal.")
    else:
        takeaway_bits.append("TfL currently has the stronger punctuality signal.")
else:
    takeaway_bits.append("Use MTA punctuality carefully until coverage improves.")

operator_takeaway = " ".join(takeaway_bits) if takeaway_bits else "No strong network-level difference is available yet."

aligned_metric_rows = [
    {"Metric": "Impacted Service Rate %", "MTA": mta_impacted_rate, "TfL": tfl_impacted_rate},
    {"Metric": "Avg Headway (min)", "MTA": mta_avg_headway, "TfL": tfl_avg_headway},
    {"Metric": "Locations per Service", "MTA": mta_locations_per_service, "TfL": tfl_locations_per_service},
]
if mta_punctuality_pct is not None and tfl_punctuality_pct is not None:
    aligned_metric_rows.append({"Metric": "Punctuality %", "MTA": mta_punctuality_pct, "TfL": tfl_punctuality_pct})

aligned_metric_df = pd.DataFrame(aligned_metric_rows)
aligned_metric_df["SortValue"] = aligned_metric_df[["MTA", "TfL"]].mean(axis=1)

scale_context_df = pd.DataFrame([
    {"Metric": "Freshness (s)", "MTA": mta_source_latency, "TfL": tfl_fresh_s},
    {"Metric": "Active Services", "MTA": mta_active_services, "TfL": tfl_active_services},
    {"Metric": "Active Locations", "MTA": mta_active_locations, "TfL": tfl_active_locations},
])

# -----------------------------------------------------------------------------
# Build unified watchlist
# -----------------------------------------------------------------------------

watchlist_parts = []

if not tfl_watchlist.empty:
    tfl_watchlist["SECONDARY_RISK_PCT"] = tfl_watchlist["GHOST_RATE_PCT"].fillna(0.0)
    tfl_watchlist["TRACKED_ACTIVITY"] = tfl_watchlist["TRACKED_ARRIVALS"]
    watchlist_parts.append(
        tfl_watchlist[[
            "NETWORK", "SERVICE_LABEL", "SERVICE_ID", "SERVICE_TYPE", "ACTIVE_TRAINS", "ACTIVE_LOCATIONS",
            "AVG_HEADWAY_MINS", "MEDIAN_HEADWAY_MINS", "PUNCTUALITY_PCT", "PUNCTUALITY_COVERAGE_PCT",
            "IMPACTED_FLAG", "STATUS_LABEL", "SECONDARY_RISK_PCT", "TRACKED_ACTIVITY", "PUNCTUALITY_SAMPLE",
            "CONFIDENCE_LABEL",
        ]].copy()
    )

if not mta_watchlist.empty:
    for col in ["ACTIVE_LOCATIONS", "PUNCTUALITY_PCT", "PUNCTUALITY_COVERAGE_PCT", "OBSERVED_DELAY_EVENTS"]:
        if col not in mta_watchlist.columns:
            mta_watchlist[col] = pd.NA

    mta_watchlist["SECONDARY_RISK_PCT"] = (mta_watchlist["AVG_MINUTES_AWAY"].fillna(0) / 20 * 100).clip(0, 100)
    mta_watchlist["TRACKED_ACTIVITY"] = mta_watchlist["FUTURE_STOP_PREDICTIONS"]
    watchlist_parts.append(
        mta_watchlist[[
            "NETWORK", "SERVICE_LABEL", "ROUTE_ID", "SERVICE_TYPE", "ACTIVE_TRAINS", "ACTIVE_LOCATIONS",
            "AVG_HEADWAY_MINS", "MEDIAN_MINUTES_AWAY", "PUNCTUALITY_PCT", "PUNCTUALITY_COVERAGE_PCT",
            "IMPACTED_FLAG", "STATUS_LABEL", "SECONDARY_RISK_PCT", "TRACKED_ACTIVITY", "OBSERVED_DELAY_EVENTS",
            "CONFIDENCE_LABEL",
        ]].rename(columns={
            "ROUTE_ID": "SERVICE_ID",
            "MEDIAN_MINUTES_AWAY": "MEDIAN_HEADWAY_MINS",
            "OBSERVED_DELAY_EVENTS": "PUNCTUALITY_SAMPLE",
        }).copy()
    )

watchlist_df = pd.concat(watchlist_parts, ignore_index=True) if watchlist_parts else pd.DataFrame()

if not watchlist_df.empty:
    watchlist_df["HEADWAY_SCORE"] = add_norm_score(
        watchlist_df["AVG_HEADWAY_MINS"].fillna(
            watchlist_df["AVG_HEADWAY_MINS"].median() if watchlist_df["AVG_HEADWAY_MINS"].notna().any() else 0
        )
    )
    watchlist_df["PUNCTUALITY_SCORE"] = ((100 - watchlist_df["PUNCTUALITY_PCT"].fillna(75)) / 100).clip(0, 1)
    watchlist_df["IMPACT_SCORE"] = watchlist_df["IMPACTED_FLAG"].fillna(0).astype(float)
    watchlist_df["SECONDARY_SCORE"] = (watchlist_df["SECONDARY_RISK_PCT"].fillna(0) / 100).clip(0, 1)
    watchlist_df["LOW_CONFIDENCE_PENALTY"] = watchlist_df["CONFIDENCE_LABEL"].map({
        "High": 0.00, "Medium": 0.05, "Low": 0.10, "Experimental": 0.15,
    }).fillna(0.0)
    watchlist_df["BADNESS_SCORE"] = (
        0.35 * watchlist_df["PUNCTUALITY_SCORE"]
        + 0.25 * watchlist_df["HEADWAY_SCORE"]
        + 0.25 * watchlist_df["IMPACT_SCORE"]
        + 0.15 * watchlist_df["SECONDARY_SCORE"]
        + watchlist_df["LOW_CONFIDENCE_PENALTY"]
    )
    watchlist_df = watchlist_df.sort_values("BADNESS_SCORE", ascending=False).reset_index(drop=True)

# -----------------------------------------------------------------------------
# Trend data
# -----------------------------------------------------------------------------

trend_options = {}

if not tfl_network["trend_impacted"].empty and not mta_network["alert_trend"].empty and not mta_network["snapshot_trend"].empty:
    mta_impacted_trend = mta_network["alert_trend"].merge(mta_network["snapshot_trend"], on="HOUR_BUCKET", how="outer")
    mta_impacted_trend["METRIC_VALUE"] = mta_impacted_trend["IMPACTED_SERVICES"] / mta_impacted_trend["ACTIVE_SERVICES"] * 100
    mta_impacted_trend = mta_impacted_trend[["HOUR_BUCKET", "METRIC_VALUE"]].copy()
    trend_options["Impacted Service Rate %"] = {
        "TfL": tfl_network["trend_impacted"].copy(),
        "MTA": mta_impacted_trend.copy(),
    }

if not tfl_network["trend_headway"].empty and not mta_network["headway_recent"].empty:
    mta_headway_trend = (
        mta_network["headway_recent"]
        .groupby("HOUR_BUCKET", as_index=False)["AVG_HEADWAY_MINS"]
        .mean()
        .rename(columns={"AVG_HEADWAY_MINS": "METRIC_VALUE"})
    )
    trend_options["Avg Headway (min)"] = {
        "TfL": tfl_network["trend_headway"].copy(),
        "MTA": mta_headway_trend.copy(),
    }

if not tfl_network["trend_punctuality"].empty and not mta_punctuality_trend.empty:
    trend_options["Punctuality %"] = {
        "TfL": tfl_network["trend_punctuality"].copy(),
        "MTA": mta_punctuality_trend[["HOUR_BUCKET", "METRIC_VALUE"]].copy(),
    }

# -----------------------------------------------------------------------------
# Detail prep
# -----------------------------------------------------------------------------

tfl_lag_line = latest_scalar(tfl_detail["lag_line"], "LINE_ID")
tfl_lag_value = latest_scalar(tfl_detail["lag_line"], "AVG_ERROR")
tfl_on_time = tfl_detail["on_time"].copy()
tfl_reliability = tfl_detail["reliability"].copy()
tfl_ghost_trains = tfl_detail["ghost_trains"].copy()
tfl_headway = tfl_detail["headway"].copy()
tfl_gold_sample = tfl_detail["gold_sample"].copy()
tfl_jitter = tfl_detail["jitter"].copy()
tfl_crowding = tfl_detail["crowding"].copy()
tfl_lift = tfl_detail["lift"].copy()
tfl_lift_trend = tfl_detail["lift_trend"].copy()
tfl_ticker = tfl_detail["ticker"].copy()

mta_detail_route_service = mta_detail["route_service"].copy()
mta_stop_board = mta_detail["stop_board"].copy()
mta_stop_leaderboard = mta_detail["stop_leaderboard"].copy()
mta_detail_headway = mta_detail["headway_recent"].copy()
mta_detail_alert_trend = mta_detail["alert_trend"].copy()
mta_detail_punctuality = mta_detail["punctuality_route"].copy()

if not mta_detail_route_service.empty and not mta_detail_punctuality.empty:
    mta_detail_route_service = mta_detail_route_service.merge(
        mta_detail_punctuality, on=["ROUTE_ID", "DIRECTION"], how="left",
    )

if not mta_detail_route_service.empty:
    mta_detail_route_service["SERVICE_LABEL"] = (
        mta_detail_route_service["ROUTE_ID"].astype(str)
        + " · "
        + mta_detail_route_service["DIRECTION"].fillna("Unknown").astype(str)
    )

if not mta_stop_board.empty:
    mta_stop_board["NEXT_EVENT_TS"] = pd.to_datetime(mta_stop_board["NEXT_EVENT_TS"])

if not tfl_crowding.empty:
    tfl_crowding["OBSERVATION_TIME_UTC"] = pd.to_datetime(tfl_crowding["OBSERVATION_TIME_UTC"])

if not tfl_lift_trend.empty:
    tfl_lift_trend["HOUR_BUCKET"] = pd.to_datetime(tfl_lift_trend["HOUR_BUCKET"])

# -----------------------------------------------------------------------------
# Tabs
# -----------------------------------------------------------------------------

comparison_tab, watchlist_tab, tfl_tab, mta_tab, confidence_tab = st.tabs(
    ["Network Comparison", "Service Watchlist", "TfL Detail", "MTA Detail", "Data Confidence"]
)

# -----------------------------------------------------------------------------
# Comparison tab
# -----------------------------------------------------------------------------

with comparison_tab:
    st.subheader("At a Glance")
    st.caption("Start here to compare current network conditions across MTA and TfL.")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### MTA")
        st.metric("Status", mta_status)
        st.metric("Impacted Service Rate", safe_pct(mta_impacted_rate))
        st.metric("Avg Headway", fmt_metric(mta_avg_headway, 2, " min"))
        st.metric("On-Time %", mta_punctuality_display)
    with c2:
        st.markdown("### TfL")
        st.metric("Status", tfl_status)
        st.metric("Impacted Service Rate", safe_pct(tfl_impacted_rate))
        st.metric("Avg Headway", fmt_metric(tfl_avg_headway, 2, " min"))
        st.metric("On-Time %", safe_pct(tfl_punctuality_pct))

    st.divider()
    st.subheader("Shared Performance Gap")
    if not aligned_metric_df.empty:
        fig = make_dumbbell_chart(aligned_metric_df, "MTA", "TfL")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No aligned metrics available.")

    st.divider()
    col3, col4 = st.columns([1.25, 1])

    with col3:
        st.subheader("Comparison Trend")
        if trend_options:
            selected_metric = st.radio("Compare over time:", list(trend_options.keys()), horizontal=True)
            tfl_trend_df = trend_options[selected_metric]["TfL"].copy()
            mta_trend_df = trend_options[selected_metric]["MTA"].copy()
            tfl_trend_df["NETWORK"] = "TfL"
            mta_trend_df["NETWORK"] = "MTA"
            trend_df = pd.concat([tfl_trend_df, mta_trend_df], ignore_index=True)
            trend_df["HOUR_BUCKET"] = pd.to_datetime(trend_df["HOUR_BUCKET"], utc=True).dt.tz_localize(None)
            fig = px.line(
                trend_df, x="HOUR_BUCKET", y="METRIC_VALUE", color="NETWORK", markers=True,
                labels={"HOUR_BUCKET": "Hour", "METRIC_VALUE": selected_metric},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Trend comparison becomes available once both networks have aligned hourly metrics.")

    with col4:
        st.subheader("Scale & Context")
        scale_display = scale_context_df.copy()
        for col in ["MTA", "TfL"]:
            scale_display[col] = scale_display[col].apply(lambda v: round(v, 2) if pd.notna(v) else v)
        st.dataframe(scale_display, hide_index=True, use_container_width=True)
        st.caption(
            "These are context metrics, not direct winners/losers. "
            "Use the shared performance gap and trend chart for the operational story."
        )


# -----------------------------------------------------------------------------
# Service Watchlist
# -----------------------------------------------------------------------------

with watchlist_tab:
    st.subheader("Shared Service Watchlist")
    st.caption("Ranks lines/routes that look most at risk now using punctuality, headway, disruptions, and a network-specific stress signal.")

    if watchlist_df.empty:
        st.info("No watchlist rows available.")
    else:
        col1, col2 = st.columns([1.05, 1.2])

        with col1:
            worst_df = watchlist_df.head(top_n_services).copy()
            fig = px.bar(
                worst_df.sort_values("BADNESS_SCORE", ascending=True),
                x="BADNESS_SCORE", y="SERVICE_LABEL", color="NETWORK",
                orientation="h", text="STATUS_LABEL",
                labels={"BADNESS_SCORE": "Risk Score", "SERVICE_LABEL": ""},
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            display_df = watchlist_df[[
                "NETWORK", "SERVICE_LABEL", "STATUS_LABEL", "PUNCTUALITY_PCT",
                "PUNCTUALITY_COVERAGE_PCT", "AVG_HEADWAY_MINS", "ACTIVE_TRAINS",
                "TRACKED_ACTIVITY", "BADNESS_SCORE", "CONFIDENCE_LABEL",
            ]].copy()
            for col in ["PUNCTUALITY_PCT", "PUNCTUALITY_COVERAGE_PCT", "AVG_HEADWAY_MINS", "BADNESS_SCORE"]:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)
            st.dataframe(display_df.head(25), hide_index=True, use_container_width=True)

        st.divider()
        filter_choice = st.radio("Show watchlist for", ["Both networks", "MTA only", "TfL only"], horizontal=True)
        filtered = watchlist_df.copy()
        if filter_choice == "MTA only":
            filtered = filtered[filtered["NETWORK"] == "MTA"]
        elif filter_choice == "TfL only":
            filtered = filtered[filtered["NETWORK"] == "TfL"]

        fig = px.scatter(
            filtered, x="AVG_HEADWAY_MINS", y="BADNESS_SCORE",
            size="ACTIVE_TRAINS", color="NETWORK", symbol="STATUS_LABEL",
            hover_name="SERVICE_LABEL",
            labels={"AVG_HEADWAY_MINS": "Avg Headway (min)", "BADNESS_SCORE": "Risk Score"},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)


# -----------------------------------------------------------------------------
# TfL Detail
# -----------------------------------------------------------------------------

with tfl_tab:
    st.subheader("TfL Line Performance")

    tfl_fresh_s += 25200.0

    if tfl_fresh_s is None or pd.isna(tfl_fresh_s):
        st.warning("🟡 TfL freshness could not be computed.")
    elif tfl_fresh_s < 900:
        st.success(f"🟢 TfL feed healthy — data is about {int(tfl_fresh_s):,}s old")
    elif tfl_fresh_s < 1800:
        st.warning(f"🟡 TfL feed lagging — data is about {int(tfl_fresh_s):,}s old")
    else:
        st.error(f"🔴 TfL feed stale — data is about {int(tfl_fresh_s):,}s old")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: st.metric("Tracked Arrivals", fmt_metric(tfl_tracked_arrivals))
    with c2: st.metric("Active Services", fmt_metric(tfl_active_services))
    with c3: st.metric("Active Locations", fmt_metric(tfl_active_locations))
    with c4: st.metric("Active Trains", fmt_metric(tfl_active_trains))
    with c5: st.metric("Laggiest Service", tfl_lag_line.title() if tfl_lag_line else "—", delta=f"+{tfl_lag_value:.0f}s" if tfl_lag_value is not None else None)
    with c6: st.metric("On-Time %", safe_pct(tfl_punctuality_pct))

    c7, c8, c9, c10 = st.columns(4)
    with c7: st.metric("Ghost Rate", safe_pct(tfl_ghost_rate))
    with c8: st.metric("Avg Abs Error", fmt_metric(tfl_avg_abs_error, 1, " sec"))
    with c9: st.metric("Good Service %", safe_pct(tfl_good_service_pct))
    with c10: st.metric("Impacted Services", fmt_metric(tfl_impacted_services))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("On-Time % by Line & Period")
        if not tfl_on_time.empty:
            pivot = tfl_on_time.pivot(index="LINE_ID", columns="SERVICE_PERIOD", values="ON_TIME_PCT")
            col_order = [c for c in ["AM Peak", "Off-Peak", "PM Peak"] if c in pivot.columns]
            if col_order:
                pivot = pivot[col_order]
            fig = px.imshow(
                pivot, color_continuous_scale="RdYlGn", range_color=[50, 100],
                text_auto=".1f", labels={"color": "On-Time %"},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL on-time rows found.")

    with col2:
        st.subheader("Ghost Rate by Line")
        if not tfl_ghost_trains.empty:
            gt = tfl_ghost_trains.sort_values("GHOST_PCT", ascending=True)
            fig = px.bar(
                gt, x="GHOST_PCT", y="LINE_ID", orientation="h",
                color="GHOST_PCT", labels={"GHOST_PCT": "Ghost %", "LINE_ID": ""}, text="GHOST_PCT",
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL ghost-rate rows found.")

    st.divider()
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Reliability by Hour")
        if not tfl_reliability.empty:
            # CLEAN schema has LINE_ID as the identifier column
            pivot_rel = tfl_reliability.pivot(index="LINE_ID", columns="HR", values="RELIABILITY_PCT")
            fig = px.imshow(
                pivot_rel, color_continuous_scale="RdYlGn", range_color=[0, 100],
                labels={"color": "Good Service %", "x": "Hour", "y": ""},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL reliability rows found.")

    with col4:
        st.subheader("Line Frequency & Regularity")
        if not tfl_headway.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(x=tfl_headway["LINE_ID"], y=tfl_headway["AVG_HEADWAY_MINS"], name="Avg Headway (min)"))
            fig.add_trace(go.Scatter(
                x=tfl_headway["LINE_ID"], y=tfl_headway["HEADWAY_CV_PCT"],
                name="Regularity CV %", yaxis="y2", mode="markers+lines",
            ))
            fig.update_layout(
                yaxis=dict(title="Avg Headway (min)"),
                yaxis2=dict(title="CV %", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(t=40, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL headway rows found.")

    st.divider()
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Prediction Error by Period")
        if not tfl_gold_sample.empty:
            fig = px.box(
                tfl_gold_sample, x="SERVICE_PERIOD", y="PREDICTION_ERROR_SECONDS", color="LINE_ID",
                category_orders={"SERVICE_PERIOD": ["AM Peak", "Off-Peak", "PM Peak"]},
                labels={"PREDICTION_ERROR_SECONDS": "Error (sec)", "SERVICE_PERIOD": ""},
            )
            fig.update_yaxes(range=[-300, 600])
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL prediction-error rows found.")

    with col6:
        st.subheader("Most Unstable Stations")
        if not tfl_jitter.empty:
            jt = tfl_jitter.sort_values("AVG_JITTER", ascending=True)
            fig = px.bar(
                jt, x="AVG_JITTER", y="STATION_NAME", color="LINE_ID", orientation="h",
                labels={"AVG_JITTER": "Avg Jitter (sec)", "STATION_NAME": ""},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL jitter rows found.")

    st.divider()
    col7, col8 = st.columns([3, 2])

    with col7:
        st.subheader("Crowding")
        if not tfl_crowding.empty:
            hourly = tfl_crowding.set_index("OBSERVATION_TIME_UTC").resample("h").mean(numeric_only=True).reset_index()
            fig = px.area(
                hourly, x="OBSERVATION_TIME_UTC", y="CROWDING_PERCENTAGE",
                labels={"CROWDING_PERCENTAGE": "% of Baseline", "OBSERVATION_TIME_UTC": ""},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No TfL crowding rows found.")

    with col8:
        st.subheader("Lift Disruption Trend")
        if not tfl_lift_trend.empty:
            fig = px.line(
                tfl_lift_trend, x="HOUR_BUCKET", y="DISRUPTIONS",
                labels={"DISRUPTIONS": "Lift Disruptions", "HOUR_BUCKET": ""},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No lift-trend rows found.")

    st.divider()
    col9, col10 = st.columns([3, 2])

    with col9:
        st.subheader("🕒 Live Arrival Ticker")
        if not tfl_ticker.empty:
            ticker_df = tfl_ticker.copy()
            ticker_df["STATUS"] = ticker_df["IS_ON_TIME"].map({1: "✅ On Time", 0: "⚠️ Late"})
            ticker_df = ticker_df.drop(columns=["IS_ON_TIME"])
            ticker_df["DELAY_SEC"] = ticker_df["DELAY_SEC"].apply(lambda x: f"+{x}s" if x > 0 else f"{x}s")
            st.dataframe(ticker_df, hide_index=True, use_container_width=True)
        else:
            st.info("No recent TfL ticker rows found.")

    with col10:
        st.subheader("Current Lift Outages")
        if not tfl_lift.empty:
            fig = px.bar(
                tfl_lift, x="DISRUPTION_COUNT", y="STATION_ID", orientation="h",
                color="DISRUPTION_COUNT",
                labels={"DISRUPTION_COUNT": "Disruptions", "STATION_ID": ""},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No current lift-outage rows found.")


# -----------------------------------------------------------------------------
# MTA Detail (unchanged)
# -----------------------------------------------------------------------------

with mta_tab:
    st.subheader("MTA Route Performance")

    if mta_source_latency is None or pd.isna(mta_source_latency):
        st.warning("🟡 MTA source freshness could not be computed.")
    elif mta_source_latency < 6000:
        st.success(f"🟢 MTA source healthy — latest source data is about {int(mta_source_latency):,}s old")
    elif mta_source_latency < 12000:
        st.warning(f"🟡 MTA source lagging — latest source data is about {int(mta_source_latency):,}s old")
    else:
        st.error(f"🔴 MTA source stale — latest source data is about {int(mta_source_latency):,}s old")

    mta_gold_latency = safe_float(latest_scalar(mta_fresh, "GOLD_LOAD_LATENCY_SECONDS"))
    if mta_gold_latency is not None:
        st.caption(f"Gold load history latency: {int(mta_gold_latency):,} seconds")

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Active Services", fmt_metric(mta_active_services))
    with c2: st.metric("Active Locations", fmt_metric(mta_active_locations))
    with c3: st.metric("Future Predictions", fmt_metric(mta_future_predictions))
    with c4: st.metric("Impacted Services", fmt_metric(mta_impacted_services))
    with c5: st.metric("Avg Headway", fmt_metric(mta_avg_headway, 2, " min"))

    c6, c7, c8, c9 = st.columns(4)
    with c6: st.metric("Avg Wait", fmt_metric(mta_avg_wait, 2, " min"))
    with c7: st.metric("Observed Punctuality", mta_punctuality_display)
    with c8: st.metric("Punctuality Coverage", safe_pct(mta_punctuality_coverage))
    with c9: st.metric("Soonest Train", fmt_metric(latest_scalar(mta_summary, "SOONEST_TRAIN_MIN"), 0, " min"))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Current Route Health")
        if not mta_detail_route_service.empty:
            chart_df = mta_detail_route_service.sort_values(
                ["ACTIVE_ALERTS", "AVG_HEADWAY_MINS", "AVG_MINUTES_AWAY"],
                ascending=[False, False, False],
            ).head(top_n_services)
            bar_col = "ACTIVE_TRAINS" if show_mta_active_trains else "FUTURE_STOP_PREDICTIONS"
            bar_label = "Active Trains" if show_mta_active_trains else "Future Predictions"
            fig = go.Figure()
            fig.add_trace(go.Bar(x=chart_df["SERVICE_LABEL"], y=chart_df[bar_col], name=bar_label))
            fig.add_trace(go.Scatter(
                x=chart_df["SERVICE_LABEL"], y=chart_df["AVG_HEADWAY_MINS"],
                name="Avg Headway (min)", yaxis="y2", mode="markers+lines",
            ))
            fig.update_layout(
                xaxis_title="Route / Direction",
                yaxis=dict(title=bar_label),
                yaxis2=dict(title="Avg Headway (min)", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(t=40, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No MTA route-service rows found.")

    with col2:
        st.subheader("Wait vs Service Coverage")
        if not mta_detail_route_service.empty:
            fig = px.scatter(
                mta_detail_route_service,
                x="AVG_MINUTES_AWAY", y="FUTURE_STOP_PREDICTIONS",
                size="ACTIVE_TRAINS", color="ACTIVE_ALERTS", hover_name="SERVICE_LABEL",
                labels={"AVG_MINUTES_AWAY": "Avg Wait (min)", "FUTURE_STOP_PREDICTIONS": "Future Stop Predictions", "ACTIVE_ALERTS": "Active Alerts"},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No MTA route-service rows found for scatter view.")

    st.divider()
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Route Headways")
        if not mta_detail_headway.empty:
            mta_detail_headway["SERVICE_LABEL"] = (
                mta_detail_headway["ROUTE_ID"].astype(str)
                + " · "
                + mta_detail_headway["DIRECTION"].fillna("Unknown").astype(str)
            )
            plot_df = mta_detail_headway.sort_values("AVG_HEADWAY_MINS", ascending=True).tail(top_n_services)
            fig = px.bar(
                plot_df, x="AVG_HEADWAY_MINS", y="SERVICE_LABEL", orientation="h",
                text="AVG_HEADWAY_MINS", labels={"AVG_HEADWAY_MINS": "Avg Headway (min)", "SERVICE_LABEL": ""},
            )
            fig.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No recent MTA headway rows found.")

    with col4:
        st.subheader("Stops With Most Upcoming Service")
        if not mta_stop_leaderboard.empty:
            fig = px.bar(
                mta_stop_leaderboard.sort_values(["UPCOMING_TRAINS", "SOONEST_TRAIN_MIN"], ascending=[True, False]),
                x="UPCOMING_TRAINS", y="STOP_ID", orientation="h",
                color="AVG_WAIT_MIN", text="SOONEST_TRAIN_MIN",
                labels={"UPCOMING_TRAINS": "Upcoming Trains", "STOP_ID": "", "AVG_WAIT_MIN": "Avg Wait (min)"},
            )
            fig.update_traces(texttemplate="Soonest %{text:.0f} min", textposition="outside")
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No stop-board leaderboard rows found.")

    st.divider()
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Alert Trend")
        if not mta_detail_alert_trend.empty:
            mta_detail_alert_trend["HOUR_BUCKET"] = pd.to_datetime(mta_detail_alert_trend["HOUR_BUCKET"])
            fig = px.line(
                mta_detail_alert_trend, x="HOUR_BUCKET", y="ACTIVE_ALERTS",
                labels={"HOUR_BUCKET": "Hour", "ACTIVE_ALERTS": "Active Alerts"},
            )
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No MTA alert-trend rows found.")

    with col6:
        st.subheader("Observed Punctuality Coverage")
        if not mta_detail_route_service.empty and "COVERAGE_PCT" in mta_detail_route_service.columns:
            coverage_df = mta_detail_route_service.dropna(subset=["COVERAGE_PCT"])
            if not coverage_df.empty:
                fig = px.bar(
                    coverage_df.sort_values("COVERAGE_PCT", ascending=True).tail(top_n_services),
                    x="COVERAGE_PCT", y="SERVICE_LABEL", orientation="h",
                    color="ON_TIME_PCT_2MIN" if "ON_TIME_PCT_2MIN" in coverage_df.columns else None,
                    labels={"COVERAGE_PCT": "Coverage %", "SERVICE_LABEL": ""},
                )
                fig.update_layout(margin=dict(t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No punctuality coverage rows found for this route filter.")
        else:
            st.info("MTA punctuality coverage is unavailable until MTA_PUNCTUALITY_COVERAGE_GOLD is present.")

    st.divider()
    subtab1, subtab2 = st.tabs(["Live Stop Board", "Route Service Table"])

    with subtab1:
        st.subheader("🕒 Live Stop Board")
        if not mta_stop_board.empty:
            board_df = mta_stop_board.copy()
            board_df["NEXT_EVENT_TS"] = board_df["NEXT_EVENT_TS"].dt.strftime("%Y-%m-%d %H:%M:%S")
            board_df["MINUTES_AWAY"] = board_df["MINUTES_AWAY"].apply(lambda v: f"{int(v)} min" if pd.notna(v) else "—")
            st.dataframe(board_df, hide_index=True, use_container_width=True)
        else:
            st.info("No stop-board rows found for the current filters.")

    with subtab2:
        st.subheader("Current Route Service Table")
        if not mta_detail_route_service.empty:
            display_df = mta_detail_route_service.copy()
            for col in [
                "ACTIVE_TRAINS", "ACTIVE_VEHICLE_TRIPS", "FUTURE_STOP_PREDICTIONS", "NEXT_ARRIVAL_MIN",
                "AVG_MINUTES_AWAY", "MEDIAN_MINUTES_AWAY", "AVG_HEADWAY_MINS", "MIN_HEADWAY_MINS",
                "MAX_HEADWAY_MINS", "ACTIVE_ALERTS", "AVG_TRIP_UPDATES_PER_MIN",
                "AVG_VEHICLE_POSITIONS_PER_MIN", "AVG_STOP_TIME_UPDATES_PER_MIN", "ON_TIME_PCT_2MIN", "COVERAGE_PCT",
            ]:
                if col in display_df.columns:
                    display_df[col] = display_df[col].round(2)
            st.dataframe(display_df, hide_index=True, use_container_width=True)
        else:
            st.info("No MTA route-service rows found for the current filters.")


# -----------------------------------------------------------------------------
# Confidence tab
# -----------------------------------------------------------------------------

with confidence_tab:
    st.subheader("Data Confidence & Metric Interpretation")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Shared comparison principles")
        st.markdown(
            """
            - **Impacted service rate** compares disrupted services as a percentage of active services  
            - **Avg headway** compares service spacing, not total system size  
            - **Trains per service** and **locations per service** help compare network scale more fairly  
            - **Punctuality** is directly observed for TfL and coverage-aware for MTA  
            """
        )
    with col2:
        status_rows = [
            {"Object": "MTA_PUNCTUALITY_COVERAGE_GOLD", "Present": optional_tables["punctuality"]},
            {"Object": "MTA_VEHICLE_POSITIONS_SILVER", "Present": optional_tables["vehicle_positions"]},
            {"Object": "MTA_ROUTE_SERVICE_STATUS_V", "Present": optional_tables["service_status_view"]},
            {"Object": "MTA_STOPS_DIM", "Present": optional_tables["stops_dim"]},
        ]
        st.dataframe(pd.DataFrame(status_rows), hide_index=True, use_container_width=True)

    st.divider()

    if optional_tables["punctuality"] and not mta_punctuality_network.empty:
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("MTA Observed Punctuality", safe_pct(mta_punctuality_pct))
        with c2: st.metric("MTA Coverage", safe_pct(mta_punctuality_coverage))
        with c3: st.metric("Observed Delay Events", fmt_metric(latest_scalar(mta_punctuality_network, "OBSERVED_DELAY_EVENTS")))
        with c4: st.metric("Dashboard Readiness", "Ready" if mta_punctuality_ready else "Experimental")
    else:
        st.info("MTA punctuality coverage is not available yet.")

    st.divider()
    st.markdown(
        """
        **Best next model improvements**
        1. Add `MTA_STOPS_DIM` and enrich the stop board with stop names  
        2. Create `MTA_ROUTE_SERVICE_STATUS_V` and use shared status labels in the watchlist  
        3. Build expected headway baselines by hour/day for both MTA and TfL  
        4. Add a headway ratio metric: `current_headway / expected_headway`  
        5. Promote MTA punctuality higher in the comparison tab once coverage is consistently strong  
        """
    )