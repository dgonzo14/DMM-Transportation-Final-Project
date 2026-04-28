import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(layout="wide", page_title="TfL Performance Command Center")

session = get_active_session()

st.title("🚇 TfL Metro Performance Command Center")
st.caption("Real-time analytics across the London Underground network.")

# =============================================================================
# Data loading
# =============================================================================

@st.cache_data(ttl=600)
def load_dashboard_data():
    queries = {}

    queries["fresh"] = """
        SELECT DATEDIFF('second', MAX(DATA_CAPTURED_AT), CURRENT_TIMESTAMP()) AS latency
        FROM SILVER_TFL_ARRIVALS
    """

    queries["stats"] = """
        SELECT
            COUNT(DISTINCT PREDICTION_ID)  AS total_trips,
            COUNT(DISTINCT STATION_ID)     AS active_stations,
            COUNT(DISTINCT VEHICLE_ID)     AS active_trains
        FROM SILVER_TFL_ARRIVALS
    """

    queries["lag_line"] = """
        SELECT LINE_ID, ROUND(AVG(prediction_error_seconds), 1) AS avg_error
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        GROUP BY 1 ORDER BY avg_error DESC LIMIT 1
    """

    queries["on_time"] = """
        SELECT
            LINE_ID,
            SERVICE_PERIOD,
            ROUND(AVG(IS_ON_TIME) * 100, 1)  AS on_time_pct,
            COUNT(*)                          AS sample_size
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        GROUP BY 1, 2
        HAVING sample_size > 10
        ORDER BY 1, 2
    """

    queries["reliability"] = """
        SELECT LINE_NAME, HOUR(RECORD_TIMESTAMP) AS hr,
               AVG(CASE WHEN SEVERITY_CODE = 10 THEN 100 ELSE 0 END) AS reliability_pct
        FROM SILVER_TFL_LINE_STATUS GROUP BY 1, 2 ORDER BY 2
    """

    queries["ghost_trains"] = """
        SELECT
            LINE_ID,
            COUNT(*)                                                          AS total_trips,
            COUNT_IF(minutes_tracked < 2 AND prediction_error_seconds > 60)  AS ghost_count,
            ROUND(ghost_count / NULLIF(total_trips, 0) * 100, 2)             AS ghost_pct,
            ROUND(AVG(minutes_tracked), 2)                                    AS avg_visibility_mins
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        GROUP BY 1 ORDER BY ghost_pct DESC
    """

    queries["headway"] = """
        WITH arrival_times AS (
            SELECT LINE_ID, STATION_NAME, actual_arrival_ts,
                LAG(actual_arrival_ts) OVER (
                    PARTITION BY LINE_ID, STATION_NAME
                    ORDER BY actual_arrival_ts ASC
                ) AS prev_arrival_ts
            FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        )
        SELECT
            LINE_ID,
            ROUND(AVG(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)) / 60.0, 2) AS avg_headway_mins,
            ROUND(STDDEV(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)) / 60.0, 2) AS headway_stddev,
            ROUND(
                STDDEV(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)) /
                NULLIF(AVG(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)), 0) * 100,
                1
            ) AS headway_cv_pct
        FROM arrival_times
        WHERE prev_arrival_ts IS NOT NULL
        GROUP BY 1
    """

    queries["gold_sample"] = """
        SELECT LINE_ID, PREDICTION_ERROR_SECONDS, SERVICE_PERIOD, IS_ON_TIME
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
    """

    queries["jitter"] = """
        SELECT STATION_NAME, LINE_ID,
               ROUND(AVG(prediction_jitter), 2) AS avg_jitter,
               COUNT(*)                          AS sample_size
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        GROUP BY 1, 2 HAVING sample_size > 5
        ORDER BY avg_jitter DESC LIMIT 15
    """

    queries["crowding"] = """
        SELECT OBSERVATION_TIME_UTC, CROWDING_PERCENTAGE
        FROM SILVER_TFL_CROWDING ORDER BY OBSERVATION_TIME_UTC ASC
    """

    queries["lift"] = """
        SELECT STATION_ID, COUNT(DISTINCT LIFT_ID) AS disruption_count
        FROM SILVER_TFL_LIFT_DISRUPTIONS GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """

    queries["lift_trend"] = """
        SELECT DATE_TRUNC('hour', record_timestamp) AS hour_bucket,
               COUNT(DISTINCT lift_id)              AS disruptions
        FROM SILVER_TFL_LIFT_DISRUPTIONS
        GROUP BY 1 ORDER BY 1
    """

    queries["ticker"] = """
        SELECT
            actual_arrival_ts::TIME  AS time,
            LINE_ID,
            STATION_NAME,
            prediction_error_seconds AS delay_sec,
            IS_ON_TIME
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        ORDER BY actual_arrival_ts DESC LIMIT 10
    """

    return {k: session.sql(v).to_pandas() for k, v in queries.items()}


data = load_dashboard_data()

# =============================================================================
# System health banner
# =============================================================================

latency = data["fresh"]["LATENCY"].iloc[0]
if latency < 60:
    st.success(f"🟢 Pipeline healthy — data {latency:.0f}s old")
elif latency < 180:
    st.warning(f"🟡 Data lagging — {latency:.0f}s behind")
else:
    st.error(f"🔴 Pipeline stalled — {latency:.0f}s behind")

# =============================================================================
# Top KPIs
# =============================================================================

stats = data["stats"].iloc[0]
lag_row = data["lag_line"].iloc[0] if not data["lag_line"].empty else None
on_time_overall = data["gold_sample"]["IS_ON_TIME"].mean() * 100

ghost_total = data["ghost_trains"]["GHOST_COUNT"].sum()
ghost_rate = (ghost_total / data["ghost_trains"]["TOTAL_TRIPS"].sum() * 100) if data["ghost_trains"]["TOTAL_TRIPS"].sum() > 0 else 0

c1, c2, c3, c4, c5, c6 = st.columns(6)
with c1:
    st.metric("Trips Tracked", f"{int(stats['TOTAL_TRIPS']):,}")
with c2:
    st.metric("Active Stations", int(stats['ACTIVE_STATIONS']))
with c3:
    st.metric("Active Trains", int(stats['ACTIVE_TRAINS']))
with c4:
    st.metric("Network On-Time %", f"{on_time_overall:.1f}%")
with c5:
    lag_label = lag_row['LINE_ID'].title() if lag_row is not None else "—"
    lag_val = f"+{lag_row['AVG_ERROR']:.0f}s" if lag_row is not None else "—"
    st.metric("Laggiest Line", lag_label, delta=lag_val, delta_color="inverse")
with c6:
    st.metric("Ghost Train Rate", f"{ghost_rate:.1f}%")

st.divider()

# =============================================================================
# Row 1: On-time % heatmap + Ghost trains
# =============================================================================

col1, col2 = st.columns(2)

with col1:
    st.subheader("On-Time % by Line & Period")
    ot = data["on_time"]
    if not ot.empty:
        pivot = ot.pivot(index="LINE_ID", columns="SERVICE_PERIOD", values="ON_TIME_PCT")
        col_order = [c for c in ["AM Peak", "Off-Peak", "PM Peak"] if c in pivot.columns]
        pivot = pivot[col_order]
        fig = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn",
            range_color=[50, 100],
            text_auto=".1f",
            labels={"color": "On-Time %"},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Ghost Train Rate by Line")
    gt = data["ghost_trains"].sort_values("GHOST_PCT", ascending=True)
    fig = px.bar(
        gt, x="GHOST_PCT", y="LINE_ID", orientation="h",
        color="GHOST_PCT", color_continuous_scale="OrRd",
        labels={"GHOST_PCT": "Ghost Train %", "LINE_ID": ""},
        text="GHOST_PCT",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# =============================================================================
# Row 2: Reliability heatmap + Headway
# =============================================================================

col3, col4 = st.columns(2)

with col3:
    st.subheader("Line Reliability by Hour")
    rel = data["reliability"]
    if not rel.empty:
        pivot_rel = rel.pivot(index="LINE_NAME", columns="HR", values="RELIABILITY_PCT")
        fig = px.imshow(
            pivot_rel,
            color_continuous_scale="RdYlGn",
            range_color=[0, 100],
            labels={"color": "Good Service %", "x": "Hour (UTC)", "y": ""},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Service Frequency & Regularity")
    hw = data["headway"]
    if not hw.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=hw["LINE_ID"], y=hw["AVG_HEADWAY_MINS"],
            name="Avg Headway (min)",
            marker_color="steelblue",
        ))
        fig.add_trace(go.Scatter(
            x=hw["LINE_ID"], y=hw["HEADWAY_CV_PCT"],
            name="Regularity CV %", yaxis="y2",
            mode="markers+lines",
            marker=dict(size=8, color="tomato"),
            line=dict(dash="dot"),
        ))
        fig.update_layout(
            yaxis=dict(title="Avg Headway (min)"),
            yaxis2=dict(title="CV % (lower = more regular)", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# =============================================================================
# Row 3: Peak penalty box plot + Jitter
# =============================================================================

col5, col6 = st.columns(2)

with col5:
    st.subheader("Prediction Error by Period")
    gd = data["gold_sample"]
    if not gd.empty:
        fig = px.box(
            gd, x="SERVICE_PERIOD", y="PREDICTION_ERROR_SECONDS", color="LINE_ID",
            category_orders={"SERVICE_PERIOD": ["AM Peak", "Off-Peak", "PM Peak"]},
            labels={"PREDICTION_ERROR_SECONDS": "Error (seconds)", "SERVICE_PERIOD": ""},
        )
        fig.update_yaxes(range=[-300, 600])
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("Prediction Jitter — Most Unstable Stations")
    jt = data["jitter"].sort_values("AVG_JITTER", ascending=True)
    fig = px.bar(
        jt, x="AVG_JITTER", y="STATION_NAME", color="LINE_ID", orientation="h",
        labels={"AVG_JITTER": "Avg Jitter (sec)", "STATION_NAME": ""},
    )
    fig.update_layout(margin=dict(t=20, b=20))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# =============================================================================
# Row 4: Crowding + Lift disruptions (trend + current)
# =============================================================================

col7, col8 = st.columns([3, 2])

with col7:
    st.subheader("Crowding & Lift Disruptions Over Time")
    tab1, tab2 = st.tabs(["Crowding Trend", "Lift Disruption Trend"])
    with tab1:
        cr = data["crowding"].copy()
        if not cr.empty:
            cr["OBSERVATION_TIME_UTC"] = pd.to_datetime(cr["OBSERVATION_TIME_UTC"])
            hourly = cr.set_index("OBSERVATION_TIME_UTC").resample("h").mean().reset_index()
            fig = px.area(
                hourly, x="OBSERVATION_TIME_UTC", y="CROWDING_PERCENTAGE",
                labels={"CROWDING_PERCENTAGE": "% of Baseline", "OBSERVATION_TIME_UTC": ""},
            )
            fig.add_hline(y=100, line_dash="dot", line_color="red", annotation_text="Baseline")
            st.plotly_chart(fig, use_container_width=True)
    with tab2:
        lt = data["lift_trend"].copy()
        if not lt.empty:
            lt["HOUR_BUCKET"] = pd.to_datetime(lt["HOUR_BUCKET"])
            fig = px.line(
                lt, x="HOUR_BUCKET", y="DISRUPTIONS",
                labels={"DISRUPTIONS": "Active Lift Disruptions", "HOUR_BUCKET": ""},
            )
            st.plotly_chart(fig, use_container_width=True)

with col8:
    st.subheader("Current Lift Outages")
    lf = data["lift"]
    if not lf.empty:
        fig = px.bar(
            lf, x="DISRUPTION_COUNT", y="STATION_ID", orientation="h",
            color="DISRUPTION_COUNT", color_continuous_scale="Reds",
            labels={"DISRUPTION_COUNT": "Disruptions", "STATION_ID": ""},
        )
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# =============================================================================
# Live ticker
# =============================================================================

st.subheader("🕒 Live Arrival Ticker")
ticker = data["ticker"].copy()
if not ticker.empty:
    ticker["STATUS"] = ticker["IS_ON_TIME"].map({1: "✅ On Time", 0: "⚠️ Late"})
    ticker = ticker.drop(columns=["IS_ON_TIME"])
    ticker["DELAY_SEC"] = ticker["DELAY_SEC"].apply(lambda x: f"+{x}s" if x > 0 else f"{x}s")
    st.dataframe(ticker, hide_index=True, use_container_width=True)