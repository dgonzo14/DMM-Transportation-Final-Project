import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.context import get_active_session


st.set_page_config(layout="wide", page_title="MTA Performance Command Center")

session = get_active_session()

TARGET_DATABASE = "GORILLA_DB"
TARGET_SCHEMA = "GORILLA_SCHEMA"
TABLE_PREFIX = f"{TARGET_DATABASE}.{TARGET_SCHEMA}."

REQUIRED_TABLES = [
    "MTA_BRONZE_LOAD_HISTORY",
    "MTA_SILVER_LOAD_HISTORY",
    "MTA_GOLD_LOAD_HISTORY",
    "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD",
    "MTA_ROUTE_HEADWAYS_GOLD",
    "MTA_ARRIVAL_INFERENCE_GOLD",
    "MTA_ALERT_ACTIVITY_GOLD",
    "MTA_ALERTS_SILVER",
]


def qname(table_name: str) -> str:
    return f"{TABLE_PREFIX}{table_name}"


@st.cache_data(ttl=600)
def get_available_tables() -> pd.DataFrame:
    return session.sql(
        f"""
        SELECT TABLE_NAME
        FROM {TARGET_DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
        ORDER BY TABLE_NAME
        """,
        [TARGET_SCHEMA],
    ).to_pandas()


available_tables_df = get_available_tables()
available_tables = set(available_tables_df["TABLE_NAME"].tolist()) if not available_tables_df.empty else set()
missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in available_tables]

if missing_tables:
    st.error(
        "This app can connect to Snowflake, but it cannot see the expected MTA pipeline tables "
        f"in {TARGET_DATABASE}.{TARGET_SCHEMA}."
    )
    st.write("Missing or inaccessible tables:")
    st.code("\n".join(missing_tables))
    st.write("Visible tables in the configured schema:")
    st.dataframe(available_tables_df, use_container_width=True)
    st.info(
        "If your pipeline tables live in another schema, update TARGET_DATABASE and TARGET_SCHEMA near "
        "the top of this file. If the names are correct, your Streamlit role likely needs USAGE on the "
        "database/schema and SELECT on the pipeline tables."
    )
    st.stop()


st.title("🚇 MTA Performance Command Center")
st.caption("Real-time analytics across the NYC subway network from the MTA bronze → silver → gold pipeline.")


with st.sidebar:
    st.header("Dashboard Controls")
    lookback_hours = st.slider(
        "Lookback window (hours)",
        min_value=2,
        max_value=72,
        value=24,
        help="Controls how much recent silver/gold activity to include in the dashboard.",
    )


@st.cache_data(ttl=600)
def load_route_options() -> pd.DataFrame:
    return session.sql(
        f"""
        SELECT DISTINCT ROUTE_ID
        FROM {qname('MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD')}
        WHERE SNAPSHOT_TS >= DATEADD('day', -7, CURRENT_TIMESTAMP())
          AND ROUTE_ID IS NOT NULL
        ORDER BY 1
        """
    ).to_pandas()


route_options_df = load_route_options()
route_options = ["All routes"] + route_options_df["ROUTE_ID"].dropna().astype(str).tolist()

with st.sidebar:
    selected_route = st.selectbox(
        "Route focus",
        options=route_options,
        index=0,
        help="Filters route-level charts while keeping top-line health metrics network-wide.",
    )
    st.caption(f"Target schema: {TARGET_DATABASE}.{TARGET_SCHEMA}")


@st.cache_data(ttl=600)
def load_dashboard_data(lookback: int, route_filter: str):
    queries = {}

    route_clause = "(? = 'All routes' OR ROUTE_ID = ?)"

    queries["fresh"] = f"""
        SELECT DATEDIFF('second', MAX(LOADED_AT), CURRENT_TIMESTAMP()) AS LATENCY
        FROM {qname('MTA_GOLD_LOAD_HISTORY')}
        WHERE STATUS = 'SUCCESS'
    """

    queries["stats"] = f"""
        WITH latest_snapshot AS (
            SELECT MAX(SNAPSHOT_TS) AS LATEST_TS
            FROM {qname('MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD')}
        )
        SELECT
            COALESCE(SUM(TRIP_UPDATE_COUNT), 0) AS TOTAL_TRIP_UPDATES,
            COUNT(DISTINCT ROUTE_ID)            AS ACTIVE_ROUTES,
            COALESCE(SUM(VEHICLE_POSITION_COUNT), 0) AS VEHICLE_PINGS,
            COALESCE(SUM(ALERT_COUNT), 0)       AS ALERT_MENTIONS
        FROM {qname('MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD')}
        WHERE SNAPSHOT_TS = (SELECT LATEST_TS FROM latest_snapshot)
    """

    queries["volatile_route"] = f"""
        SELECT
            ROUTE_ID,
            ROUND(AVG(ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0))), 1) AS AVG_ABS_DRIFT
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY AVG_ABS_DRIFT DESC
        LIMIT 1
    """

    queries["stable_pct"] = f"""
        SELECT
            ROUND(
                AVG(
                    CASE
                        WHEN ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) <= 60 THEN 1
                        ELSE 0
                    END
                ) * 100,
                1
            ) AS STABLE_PCT
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
    """

    queries["visibility_summary"] = f"""
        SELECT
            ROUND(AVG(POLL_COUNT), 2)                    AS AVG_POLLS,
            ROUND(AVG(OBSERVED_SPAN_SECONDS) / 60.0, 2) AS AVG_VISIBILITY_MINS
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
    """

    queries["stability"] = f"""
        WITH base AS (
            SELECT
                ROUTE_ID,
                CASE
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 6 AND 9 THEN 'AM Peak'
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 16 AND 19 THEN 'PM Peak'
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 10 AND 15 THEN 'Midday'
                    ELSE 'Overnight'
                END AS SERVICE_PERIOD,
                CASE
                    WHEN ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) <= 60 THEN 1
                    ELSE 0
                END AS IS_STABLE
            FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
            WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
              AND {route_clause}
        )
        SELECT
            ROUTE_ID,
            SERVICE_PERIOD,
            ROUND(AVG(IS_STABLE) * 100, 1) AS STABILITY_PCT,
            COUNT(*) AS SAMPLE_SIZE
        FROM base
        GROUP BY 1, 2
        HAVING SAMPLE_SIZE > 10
        ORDER BY 1, 2
    """

    queries["volatile_predictions"] = f"""
        WITH base AS (
            SELECT
                ROUTE_ID,
                COUNT(*) AS TOTAL_OBS,
                COUNT_IF(ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) > 120 OR POLL_COUNT < 2) AS VOLATILE_COUNT,
                ROUND(AVG(POLL_COUNT), 2) AS AVG_POLL_COUNT
            FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
            WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
              AND {route_clause}
            GROUP BY 1
        )
        SELECT
            ROUTE_ID,
            TOTAL_OBS,
            VOLATILE_COUNT,
            ROUND(VOLATILE_COUNT / NULLIF(TOTAL_OBS, 0) * 100, 2) AS VOLATILE_PCT,
            AVG_POLL_COUNT
        FROM base
        ORDER BY VOLATILE_PCT DESC
    """

    queries["snapshot_health"] = f"""
        SELECT
            ROUTE_ID,
            EXTRACT(hour FROM SNAPSHOT_TS) AS HR,
            AVG(
                CASE
                    WHEN TRIP_UPDATE_COUNT > 0 AND VEHICLE_POSITION_COUNT > 0 AND ALERT_COUNT = 0 THEN 100
                    ELSE 0
                END
            ) AS SNAPSHOT_HEALTH_PCT
        FROM {qname('MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD')}
        WHERE SNAPSHOT_TS >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1, 2
        ORDER BY 2
    """

    queries["headway"] = f"""
        SELECT
            ROUTE_ID,
            ROUND(AVG(AVG_HEADWAY_SECONDS) / 60.0, 2) AS AVG_HEADWAY_MINS,
            ROUND(STDDEV(AVG_HEADWAY_SECONDS) / 60.0, 2) AS HEADWAY_STDDEV,
            ROUND(
                STDDEV(AVG_HEADWAY_SECONDS) / NULLIF(AVG(AVG_HEADWAY_SECONDS), 0) * 100,
                1
            ) AS HEADWAY_CV_PCT
        FROM {qname('MTA_ROUTE_HEADWAYS_GOLD')}
        WHERE WINDOW_START >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1
        ORDER BY AVG_HEADWAY_MINS DESC
    """

    queries["drift_sample"] = f"""
        WITH base AS (
            SELECT
                ROUTE_ID,
                ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) AS ABS_DRIFT_SECONDS,
                CASE
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 6 AND 9 THEN 'AM Peak'
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 16 AND 19 THEN 'PM Peak'
                    WHEN EXTRACT(hour FROM LAST_SEEN_INGESTED_AT) BETWEEN 10 AND 15 THEN 'Midday'
                    ELSE 'Overnight'
                END AS SERVICE_PERIOD
            FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
            WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
              AND {route_clause}
        ),
        top_routes AS (
            SELECT ROUTE_ID
            FROM base
            GROUP BY 1
            ORDER BY COUNT(*) DESC
            LIMIT 8
        )
        SELECT ROUTE_ID, SERVICE_PERIOD, ABS_DRIFT_SECONDS
        FROM base
        WHERE ROUTE_ID IN (SELECT ROUTE_ID FROM top_routes)
    """

    queries["unstable_stops"] = f"""
        SELECT
            STOP_ID,
            ROUTE_ID,
            ROUND(AVG(ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0))), 2) AS AVG_ABS_DRIFT,
            COUNT(*) AS SAMPLE_SIZE
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1, 2
        HAVING SAMPLE_SIZE > 5
        ORDER BY AVG_ABS_DRIFT DESC
        LIMIT 15
    """

    queries["alert_trend"] = f"""
        SELECT
            DATE_TRUNC('hour', WINDOW_START) AS HOUR_BUCKET,
            SUM(ALERT_SNAPSHOT_COUNT) AS ALERT_SNAPSHOTS,
            SUM(ACTIVE_ALERT_COUNT) AS ACTIVE_ALERTS,
            SUM(DISTINCT_ALERT_ENTITY_COUNT) AS ALERT_ENTITIES
        FROM {qname('MTA_ALERT_ACTIVITY_GOLD')}
        WHERE WINDOW_START >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1
        ORDER BY 1
    """

    queries["visibility_by_route"] = f"""
        SELECT
            ROUTE_ID,
            ROUND(AVG(POLL_COUNT), 2) AS AVG_POLLS,
            ROUND(AVG(OBSERVED_SPAN_SECONDS) / 60.0, 2) AS AVG_VISIBILITY_MINS
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1
        ORDER BY AVG_VISIBILITY_MINS DESC
        LIMIT 12
    """

    queries["current_alerts"] = f"""
        SELECT
            ROUTE_ID,
            SUM(ACTIVE_ALERT_COUNT) AS ACTIVE_ALERTS
        FROM {qname('MTA_ALERT_ACTIVITY_GOLD')}
        WHERE WINDOW_START >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        GROUP BY 1
        ORDER BY ACTIVE_ALERTS DESC
        LIMIT 12
    """

    queries["ticker"] = f"""
        SELECT
            LAST_SEEN_INGESTED_AT::TIME AS TIME,
            ROUTE_ID,
            STOP_ID,
            TRAIN_ID,
            ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) AS DRIFT_SEC,
            CASE
                WHEN ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) <= 60 THEN '✅ Stable'
                WHEN ABS(COALESCE(PREDICTION_DRIFT_SECONDS, 0)) <= 180 THEN '⚠️ Shifting'
                ELSE '🚨 Volatile'
            END AS STATUS
        FROM {qname('MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE UPDATED_AT >= DATEADD('hour', -?, CURRENT_TIMESTAMP())
          AND {route_clause}
        ORDER BY LAST_SEEN_INGESTED_AT DESC
        LIMIT 12
    """

    return {
        "fresh": session.sql(queries["fresh"]).to_pandas(),
        "stats": session.sql(queries["stats"]).to_pandas(),
        "volatile_route": session.sql(queries["volatile_route"], [lookback]).to_pandas(),
        "stable_pct": session.sql(queries["stable_pct"], [lookback]).to_pandas(),
        "visibility_summary": session.sql(queries["visibility_summary"], [lookback]).to_pandas(),
        "stability": session.sql(queries["stability"], [lookback, route_filter, route_filter]).to_pandas(),
        "volatile_predictions": session.sql(queries["volatile_predictions"], [lookback, route_filter, route_filter]).to_pandas(),
        "snapshot_health": session.sql(queries["snapshot_health"], [lookback, route_filter, route_filter]).to_pandas(),
        "headway": session.sql(queries["headway"], [lookback, route_filter, route_filter]).to_pandas(),
        "drift_sample": session.sql(queries["drift_sample"], [lookback, route_filter, route_filter]).to_pandas(),
        "unstable_stops": session.sql(queries["unstable_stops"], [lookback, route_filter, route_filter]).to_pandas(),
        "alert_trend": session.sql(queries["alert_trend"], [lookback, route_filter, route_filter]).to_pandas(),
        "visibility_by_route": session.sql(queries["visibility_by_route"], [lookback, route_filter, route_filter]).to_pandas(),
        "current_alerts": session.sql(queries["current_alerts"], [lookback, route_filter, route_filter]).to_pandas(),
        "ticker": session.sql(queries["ticker"], [lookback, route_filter, route_filter]).to_pandas(),
    }


data = load_dashboard_data(lookback_hours, selected_route)


base_latency = data["fresh"]["LATENCY"].iloc[0] if not data["fresh"].empty else None
if base_latency is None or pd.isna(base_latency):
    st.warning("🟡 Pipeline freshness could not be computed from MTA_GOLD_LOAD_HISTORY.")
elif base_latency < 900:
    st.success(f"🟢 Pipeline healthy — gold data {base_latency:.0f}s old")
elif base_latency < 1800:
    st.warning(f"🟡 Data lagging — gold layer is {base_latency:.0f}s behind")
else:
    st.error(f"🔴 Pipeline stalled — gold layer is {base_latency:.0f}s behind")


stats = data["stats"].iloc[0] if not data["stats"].empty else None
volatile_row = data["volatile_route"].iloc[0] if not data["volatile_route"].empty else None
stable_pct = data["stable_pct"]["STABLE_PCT"].iloc[0] if not data["stable_pct"].empty else None
visibility_row = data["visibility_summary"].iloc[0] if not data["visibility_summary"].empty else None

c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1:
    st.metric("Trip Updates", f"{int(stats['TOTAL_TRIP_UPDATES']):,}" if stats is not None else "—")
with c2:
    st.metric("Active Routes", int(stats["ACTIVE_ROUTES"]) if stats is not None else "—")
with c3:
    st.metric("Vehicle Pings", f"{int(stats['VEHICLE_PINGS']):,}" if stats is not None else "—")
with c4:
    st.metric("Stable Predictions %", f"{stable_pct:.1f}%" if stable_pct is not None and pd.notna(stable_pct) else "—")
with c5:
    label = volatile_row["ROUTE_ID"] if volatile_row is not None else "—"
    delta = f"+{volatile_row['AVG_ABS_DRIFT']:.0f}s drift" if volatile_row is not None else None
    st.metric("Most Volatile Route", label, delta=delta, delta_color="inverse")
with c6:
    visibility_val = visibility_row["AVG_VISIBILITY_MINS"] if visibility_row is not None else None
    st.metric("Avg Visibility / Trip", f"{visibility_val:.1f} min" if visibility_val is not None and pd.notna(visibility_val) else "—")

st.divider()


col1, col2 = st.columns(2)

with col1:
    st.subheader("Prediction Stability by Route & Period")
    stability = data["stability"]
    if not stability.empty:
        pivot = stability.pivot(index="ROUTE_ID", columns="SERVICE_PERIOD", values="STABILITY_PCT")
        col_order = [c for c in ["AM Peak", "Midday", "PM Peak", "Overnight"] if c in pivot.columns]
        pivot = pivot[col_order]
        fig = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
            text_auto=".1f",
            labels={"color": "Stable %"},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No stability rows found for the current lookback window.")

with col2:
    st.subheader("Volatile Prediction Rate by Route")
    volatile = data["volatile_predictions"].sort_values("VOLATILE_PCT", ascending=True)
    if not volatile.empty:
        fig = px.bar(
            volatile,
            x="VOLATILE_PCT",
            y="ROUTE_ID",
            orientation="h",
            color="VOLATILE_PCT",
            color_continuous_scale="OrRd",
            labels={"VOLATILE_PCT": "Volatile %", "ROUTE_ID": ""},
            text="VOLATILE_PCT",
        )
        fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No volatile prediction rows found.")

st.divider()


col3, col4 = st.columns(2)

with col3:
    st.subheader("Route Snapshot Health by Hour")
    snapshot_health = data["snapshot_health"]
    if not snapshot_health.empty:
        pivot_rel = snapshot_health.pivot(index="ROUTE_ID", columns="HR", values="SNAPSHOT_HEALTH_PCT")
        fig = px.imshow(
            pivot_rel,
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
            labels={"color": "Healthy Snapshot %", "x": "Hour (Local Session TZ)", "y": ""},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No route snapshot health rows found.")

with col4:
    st.subheader("Service Frequency & Regularity")
    hw = data["headway"]
    if not hw.empty:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=hw["ROUTE_ID"],
                y=hw["AVG_HEADWAY_MINS"],
                name="Avg Headway (min)",
                marker_color="steelblue",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=hw["ROUTE_ID"],
                y=hw["HEADWAY_CV_PCT"],
                name="Regularity CV %",
                yaxis="y2",
                mode="markers+lines",
                marker=dict(size=8, color="tomato"),
                line=dict(dash="dot"),
            )
        )
        fig.update_layout(
            yaxis=dict(title="Avg Headway (min)"),
            yaxis2=dict(title="CV % (lower = steadier)", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No route headway rows found.")

st.divider()


col5, col6 = st.columns(2)

with col5:
    st.subheader("Prediction Drift by Service Period")
    drift = data["drift_sample"]
    if not drift.empty:
        fig = px.box(
            drift,
            x="SERVICE_PERIOD",
            y="ABS_DRIFT_SECONDS",
            color="ROUTE_ID",
            category_orders={"SERVICE_PERIOD": ["AM Peak", "Midday", "PM Peak", "Overnight"]},
            labels={"ABS_DRIFT_SECONDS": "Absolute Drift (seconds)", "SERVICE_PERIOD": ""},
        )
        fig.update_yaxes(range=[0, min(900, max(300, float(drift["ABS_DRIFT_SECONDS"].quantile(0.95)) + 30))])
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No arrival inference rows found for drift analysis.")

with col6:
    st.subheader("Most Unstable Stops")
    unstable = data["unstable_stops"].sort_values("AVG_ABS_DRIFT", ascending=True)
    if not unstable.empty:
        unstable["STOP_LABEL"] = unstable["STOP_ID"].astype(str) + " (" + unstable["ROUTE_ID"].astype(str) + ")"
        fig = px.bar(
            unstable,
            x="AVG_ABS_DRIFT",
            y="STOP_LABEL",
            color="ROUTE_ID",
            orientation="h",
            labels={"AVG_ABS_DRIFT": "Avg Absolute Drift (sec)", "STOP_LABEL": ""},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No unstable-stop rows found.")

st.divider()


col7, col8 = st.columns([3, 2])

with col7:
    st.subheader("Alerts Over Time")
    alerts = data["alert_trend"].copy()
    if not alerts.empty:
        alerts["HOUR_BUCKET"] = pd.to_datetime(alerts["HOUR_BUCKET"])
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=alerts["HOUR_BUCKET"],
                y=alerts["ACTIVE_ALERTS"],
                name="Active Alerts",
                mode="lines+markers",
                line=dict(color="#d62728"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=alerts["HOUR_BUCKET"],
                y=alerts["ALERT_ENTITIES"],
                name="Alert Entities",
                mode="lines",
                line=dict(color="#ff7f0e", dash="dot"),
            )
        )
        fig.update_layout(margin=dict(t=20, b=20), yaxis=dict(title="Alert Count"))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No alert activity rows found.")

with col8:
    st.subheader("MTA Visibility Depth by Route")
    visibility = data["visibility_by_route"]
    if not visibility.empty:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=visibility["ROUTE_ID"],
                y=visibility["AVG_VISIBILITY_MINS"],
                name="Avg Visibility (min)",
                marker_color="mediumpurple",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=visibility["ROUTE_ID"],
                y=visibility["AVG_POLLS"],
                name="Avg Poll Count",
                yaxis="y2",
                mode="markers+lines",
                marker=dict(size=8, color="darkgreen"),
            )
        )
        fig.update_layout(
            yaxis=dict(title="Avg Visibility (min)"),
            yaxis2=dict(title="Avg Poll Count", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No visibility rows found.")

st.divider()


col9, col10 = st.columns([3, 2])

with col9:
    st.subheader("Current Route Alert Load")
    current_alerts = data["current_alerts"].sort_values("ACTIVE_ALERTS", ascending=True)
    if not current_alerts.empty:
        fig = px.bar(
            current_alerts,
            x="ACTIVE_ALERTS",
            y="ROUTE_ID",
            orientation="h",
            color="ACTIVE_ALERTS",
            color_continuous_scale="Reds",
            labels={"ACTIVE_ALERTS": "Active Alert Count", "ROUTE_ID": ""},
            text="ACTIVE_ALERTS",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No active alert counts found.")

with col10:
    st.subheader("Dashboard Context")
    context_df = session.sql(
        """
        SELECT
            CURRENT_DATABASE() AS DATABASE_NAME,
            CURRENT_SCHEMA() AS SCHEMA_NAME,
            CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
            CURRENT_ROLE() AS ROLE_NAME,
            CURRENT_TIMESTAMP() AS QUERY_TS
        """
    ).to_pandas()
    st.dataframe(context_df, hide_index=True, use_container_width=True)

st.divider()


st.subheader("🕒 Live Prediction Ticker")
ticker = data["ticker"].copy()
if not ticker.empty:
    ticker["DRIFT_SEC"] = ticker["DRIFT_SEC"].apply(lambda value: f"{value:.0f}s")
    st.dataframe(ticker, hide_index=True, use_container_width=True)
else:
    st.info("No recent ticker rows found.")


st.markdown(
    """
    **Tables used by this sample app**

    - `MTA_GOLD_LOAD_HISTORY`
    - `MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD`
    - `MTA_ROUTE_HEADWAYS_GOLD`
    - `MTA_ARRIVAL_INFERENCE_GOLD`
    - `MTA_ALERT_ACTIVITY_GOLD`
    - `MTA_ALERTS_SILVER`

    **MTA-specific visualizations in this version**

    - Prediction stability by route and service period
    - Volatile prediction rate by route
    - Visibility depth by route using repeated stop-time observations
    - Unstable stop leaderboard based on inferred prediction drift
    """
)
