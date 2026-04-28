import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(
    layout="wide",
    page_title="TfL Command Center",
    page_icon="🚇",
)

# ---------------------------------------------------------------------------
# Styling — industrial/utilitarian dark theme matching a real operations room
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Mono', monospace;
    background-color: #0d0f14;
    color: #c8cdd8;
}
h1, h2, h3 { font-family: 'Syne', sans-serif; letter-spacing: -0.02em; }
.block-container { padding: 1.5rem 2rem; max-width: 100%; }

/* KPI cards */
[data-testid="metric-container"] {
    background: #13161e;
    border: 1px solid #1e2330;
    border-radius: 4px;
    padding: 0.75rem 1rem;
}
[data-testid="metric-container"] label {
    font-size: 0.65rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #5a6070;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif;
    font-size: 1.6rem;
    color: #e8ecf5;
}

/* Status banner */
.health-ok    { background:#0a1f14; border-left:3px solid #00c87a; padding:.5rem 1rem; border-radius:2px; }
.health-warn  { background:#1f1a08; border-left:3px solid #f5a623; padding:.5rem 1rem; border-radius:2px; }
.health-stall { background:#1f0a0a; border-left:3px solid #e8443a; padding:.5rem 1rem; border-radius:2px; }

/* Section headers */
.section-label {
    font-size: 0.65rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #3a4055;
    border-bottom: 1px solid #1e2330;
    padding-bottom: 0.4rem;
    margin-bottom: 0.75rem;
}
</style>
""", unsafe_allow_html=True)

session = get_active_session()

# ---------------------------------------------------------------------------
# Plotly base theme
# ---------------------------------------------------------------------------
PLOT_THEME = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Mono, monospace", color="#8890a0", size=11),
    margin=dict(t=24, b=24, l=8, r=8),
    xaxis=dict(gridcolor="#1a1e2a", linecolor="#1a1e2a", zeroline=False),
    yaxis=dict(gridcolor="#1a1e2a", linecolor="#1a1e2a", zeroline=False),
    colorway=["#00c87a", "#3d8ef5", "#f5a623", "#e8443a", "#b06df5",
              "#00c8c8", "#f56342", "#a3c85a"],
)

def apply_theme(fig):
    fig.update_layout(**PLOT_THEME)
    return fig


# ---------------------------------------------------------------------------
# Data loading — all queries target Spark-written tables
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_data():
    q = {}

    # Pipeline freshness — how old is the newest silver row
    q["freshness"] = """
        SELECT DATEDIFF('second', MAX(DATA_CAPTURED_AT), CURRENT_TIMESTAMP()) AS lag_seconds
        FROM TFL_ARRIVALS_SILVER
    """

    # Silver row counts per 10-min bucket over last 3 hours
    # Shows whether the streaming pipeline is landing data at the expected cadence
    q["ingest_cadence"] = """
        SELECT
            TIMESTAMPADD('minute',
                -(MINUTE(DATA_CAPTURED_AT) % 10),
                DATE_TRUNC('minute', DATA_CAPTURED_AT)) AS bucket,
            COUNT(*)                                                        AS row_count,
            COUNT(DISTINCT STATION_ID)                                      AS stations_seen
        FROM TFL_ARRIVALS_SILVER
        WHERE DATA_CAPTURED_AT >= DATEADD('hour', -3, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """

    # Top-level KPIs from gold
    q["kpis"] = """
        SELECT
            COUNT(DISTINCT PREDICTION_ID)  AS total_predictions,
            COUNT(DISTINCT STATION_ID)     AS active_stations,
            COUNT(DISTINCT VEHICLE_ID)     AS active_vehicles,
            ROUND(AVG(IS_ON_TIME) * 100, 1) AS on_time_pct,
            ROUND(AVG(prediction_error_seconds), 1) AS avg_error_s,
            ROUND(AVG(prediction_jitter), 1) AS avg_jitter_s
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
    """

    # On-time % per line per service period
    q["on_time_heatmap"] = """
        SELECT
            LINE_ID,
            SERVICE_PERIOD,
            ROUND(AVG(IS_ON_TIME) * 100, 1) AS on_time_pct,
            COUNT(*)                          AS n
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        GROUP BY 1, 2
        HAVING n > 10
    """

    # Prediction error distribution per line (for box plot)
    q["error_dist"] = """
        SELECT LINE_ID, SERVICE_PERIOD, PREDICTION_ERROR_SECONDS
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        WHERE ABS(PREDICTION_ERROR_SECONDS) < 600
    """

    # Prediction jitter — most unstable stations
    q["jitter"] = """
        SELECT STATION_NAME, LINE_ID,
               ROUND(AVG(PREDICTION_JITTER), 1) AS avg_jitter,
               COUNT(*) AS n
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        GROUP BY 1, 2 HAVING n > 5
        ORDER BY avg_jitter DESC LIMIT 15
    """

    # Ghost trains — trips that disappeared too quickly
    q["ghost_trains"] = """
        SELECT
            LINE_ID,
            COUNT(*)                                                         AS total,
            COUNT_IF(MINUTES_TRACKED < 2 AND PREDICTION_ERROR_SECONDS > 60) AS ghosts,
            ROUND(ghosts / NULLIF(total, 0) * 100, 1)                       AS ghost_pct
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        GROUP BY 1 ORDER BY ghost_pct DESC
    """

    # Headway between consecutive arrivals per line
    q["headway"] = """
        WITH t AS (
            SELECT LINE_ID, STATION_NAME, ACTUAL_ARRIVAL_TS,
                LAG(ACTUAL_ARRIVAL_TS) OVER (
                    PARTITION BY LINE_ID, STATION_NAME
                    ORDER BY ACTUAL_ARRIVAL_TS
                ) AS prev_ts
            FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        )
        SELECT
            LINE_ID,
            ROUND(AVG(DATEDIFF('second', prev_ts, ACTUAL_ARRIVAL_TS)) / 60.0, 2) AS avg_headway_min,
            ROUND(STDDEV(DATEDIFF('second', prev_ts, ACTUAL_ARRIVAL_TS)) / 60.0, 2) AS headway_std,
            ROUND(
                STDDEV(DATEDIFF('second', prev_ts, ACTUAL_ARRIVAL_TS)) /
                NULLIF(AVG(DATEDIFF('second', prev_ts, ACTUAL_ARRIVAL_TS)), 0) * 100, 1
            ) AS headway_cv_pct
        FROM t WHERE prev_ts IS NOT NULL
        GROUP BY 1 ORDER BY avg_headway_min
    """

    # Line status reliability heatmap by hour
    q["reliability"] = """
        SELECT
            LINE_ID,
            HOUR(RECORD_TIMESTAMP) AS hr,
            ROUND(AVG(CASE WHEN SEVERITY_CODE = 10 THEN 100.0 ELSE 0 END), 1) AS good_service_pct
        FROM TFL_STATUS_SILVER
        GROUP BY 1, 2 ORDER BY 1, 2
    """

    # Line status — current disruptions
    q["disruptions"] = """
        SELECT LINE_ID, STATUS_DESCRIPTION, DISRUPTION_REASON
        FROM TFL_STATUS_SILVER
        WHERE SEVERITY_CODE < 10
          AND RECORD_TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        ORDER BY LINE_ID
    """

    # Crowding trend — resampled to 10-min buckets matching ingest cadence
    q["crowding_trend"] = """
        SELECT
            TIMESTAMPADD('minute',
                -(MINUTE(OBSERVATION_TIME_UTC) % 10),
                DATE_TRUNC('minute', OBSERVATION_TIME_UTC)) AS bucket,
            ROUND(AVG(CROWDING_PERCENTAGE) * 100, 1) AS avg_crowding_pct,
            COUNT(DISTINCT NAPTAN_ID)                 AS stations_reporting
        FROM TFL_CROWDING_SILVER
        WHERE OBSERVATION_TIME_UTC >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """

    # Lift disruptions — current outages
    q["lift_current"] = """
        SELECT STATION_ID, COUNT(DISTINCT LIFT_ID) AS lift_count
        FROM TFL_LIFT_DISRUPTIONS_SILVER
        WHERE RECORD_TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 2 DESC LIMIT 12
    """

    # Lift disruption trend — 10-min buckets
    q["lift_trend"] = """
        SELECT
            TIMESTAMPADD('minute',
                -(MINUTE(RECORD_TIMESTAMP) % 10),
                DATE_TRUNC('minute', RECORD_TIMESTAMP)) AS bucket,
            COUNT(DISTINCT LIFT_ID) AS disruptions
        FROM TFL_LIFT_DISRUPTIONS_SILVER
        WHERE RECORD_TIMESTAMP >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """

    # Live arrival ticker
    q["ticker"] = """
        SELECT
            ACTUAL_ARRIVAL_TS::TIME   AS arrival_time,
            LINE_ID,
            STATION_NAME,
            PREDICTION_ERROR_SECONDS  AS error_s,
            IS_ON_TIME,
            SERVICE_PERIOD
        FROM TFL_ARRIVAL_PERFORMANCE_GOLD
        ORDER BY ACTUAL_ARRIVAL_TS DESC LIMIT 12
    """

    return {k: session.sql(v).to_pandas() for k, v in q.items()}


data = load_data()

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("# 🚇 TfL Command Center")
st.markdown('<p style="color:#3a4055;font-size:0.75rem;margin-top:-0.75rem;">LONDON UNDERGROUND · REAL-TIME OPERATIONS</p>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Pipeline health banner — based on silver ingest lag
# ---------------------------------------------------------------------------

raw_lag = int(data["freshness"]["LAG_SECONDS"].iloc[0])

# Subtract 7 hours (7 hours * 3600 seconds/hour = 25,200 seconds)
# This likely accounts for a UTC to PDT/local timezone offset
lag = raw_lag + (7 * 3600)

# Convert to minutes for the health check
lag_min = lag / 60

if lag_min < 15:
    st.markdown(f'<div class="health-ok">🟢 Pipeline nominal — silver data {lag_min:.1f} min old · next ingest due within 10 min</div>', unsafe_allow_html=True)
elif lag_min < 30:
    st.markdown(f'<div class="health-warn">🟡 Pipeline lagging — silver data {lag_min:.1f} min old · expected ≤10 min</div>', unsafe_allow_html=True)
else:
    st.markdown(f'<div class="health-stall">🔴 Pipeline stalled — silver data {lag_min:.1f} min old · check Airflow tfl_silver DAG</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Ingest cadence sparkline — unique to streaming pipeline visibility
# Shows whether 10-min batches are landing consistently
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Ingest cadence — last 3 hours (10-min buckets)</div>', unsafe_allow_html=True)
cadence = data["ingest_cadence"].copy()
if not cadence.empty:
    cadence["BUCKET"] = pd.to_datetime(cadence["BUCKET"])
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cadence["BUCKET"], y=cadence["ROW_COUNT"],
        name="Silver rows",
        marker_color="#3d8ef5",
        opacity=0.8,
    ))
    fig.add_trace(go.Scatter(
        x=cadence["BUCKET"], y=cadence["STATIONS_SEEN"],
        name="Stations seen",
        yaxis="y2",
        mode="lines+markers",
        line=dict(color="#00c87a", width=1.5),
        marker=dict(size=4),
    ))
    apply_theme(fig)
    fig.update_layout(
        height=140,
        yaxis=dict(title="", gridcolor="#1a1e2a", linecolor="#1a1e2a"),
        yaxis2=dict(title="", overlaying="y", side="right", gridcolor="#1a1e2a"),
        legend=dict(orientation="h", y=1.3, x=0, font=dict(size=10)),
        bargap=0.15,
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------

kpis = data["kpis"].iloc[0]
k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1: st.metric("Predictions", f"{int(kpis['TOTAL_PREDICTIONS']):,}")
with k2: st.metric("Stations", int(kpis['ACTIVE_STATIONS']))
with k3: st.metric("Vehicles", int(kpis['ACTIVE_VEHICLES']))
with k4: st.metric("On-Time %", f"{kpis['ON_TIME_PCT']:.1f}%")
with k5: st.metric("Avg Error", f"{kpis['AVG_ERROR_S']:.0f}s")
with k6: st.metric("Avg Jitter", f"{kpis['AVG_JITTER_S']:.0f}s")

st.divider()

# ---------------------------------------------------------------------------
# Row 1: On-time heatmap + Error distribution
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Prediction performance</div>', unsafe_allow_html=True)
col1, col2 = st.columns(2)

with col1:
    st.markdown("**On-Time % — Line × Period**")
    ot = data["on_time_heatmap"]
    if not ot.empty:
        pivot = ot.pivot(index="LINE_ID", columns="SERVICE_PERIOD", values="ON_TIME_PCT")
        for col in ["AM Peak", "Off-Peak", "PM Peak"]:
            if col not in pivot.columns:
                pivot[col] = None
        pivot = pivot[["AM Peak", "Off-Peak", "PM Peak"]]
        fig = px.imshow(
            pivot,
            color_continuous_scale=["#e8443a", "#f5a623", "#00c87a"],
            range_color=[60, 100],
            text_auto=".1f",
        )
        fig.update_layout(**PLOT_THEME, height=320,
                          coloraxis_colorbar=dict(title="", tickfont=dict(size=9)))
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Prediction Error Distribution by Period**")
    ed = data["error_dist"]
    if not ed.empty:
        fig = px.violin(
            ed, x="SERVICE_PERIOD", y="PREDICTION_ERROR_SECONDS",
            color="SERVICE_PERIOD",
            category_orders={"SERVICE_PERIOD": ["AM Peak", "Off-Peak", "PM Peak"]},
            color_discrete_map={
                "AM Peak":  "#3d8ef5",
                "Off-Peak": "#00c87a",
                "PM Peak":  "#f5a623",
            },
            box=True,
            labels={"PREDICTION_ERROR_SECONDS": "Error (s)", "SERVICE_PERIOD": ""},
        )
        fig.update_yaxes(range=[-300, 600])
        fig.update_layout(**PLOT_THEME, height=320, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 2: Headway + Jitter
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Service regularity</div>', unsafe_allow_html=True)
col3, col4 = st.columns(2)

with col3:
    st.markdown("**Headway — Average & Regularity (CV)**")
    hw = data["headway"]
    if not hw.empty:
        hw = hw.sort_values("avg_headway_min".upper())
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=hw["AVG_HEADWAY_MIN"], y=hw["LINE_ID"],
            orientation="h", name="Avg headway (min)",
            marker_color="#3d8ef5", opacity=0.85,
        ))
        fig.add_trace(go.Scatter(
            x=hw["HEADWAY_CV_PCT"], y=hw["LINE_ID"],
            mode="markers", name="CV % (regularity)",
            xaxis="x2",
            marker=dict(color="#f5a623", size=8, symbol="diamond"),
        ))
        apply_theme(fig)
        fig.update_layout(
            height=320,
            xaxis=dict(title="Avg headway (min)", gridcolor="#1a1e2a"),
            xaxis2=dict(title="CV %", overlaying="x", side="top",
                        gridcolor="#1a1e2a", range=[0, 100]),
            legend=dict(orientation="h", y=-0.15, font=dict(size=10)),
            barmode="overlay",
        )
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.markdown("**Most Unstable Stations (Jitter)**")
    jt = data["jitter"]
    if not jt.empty:
        jt = jt.sort_values("AVG_JITTER", ascending=True)
        fig = px.bar(
            jt, x="AVG_JITTER", y="STATION_NAME", color="LINE_ID",
            orientation="h",
            labels={"AVG_JITTER": "Avg jitter (s)", "STATION_NAME": ""},
        )
        fig.update_layout(**PLOT_THEME, height=320,
                          legend=dict(orientation="h", y=-0.15, font=dict(size=10)))
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 3: Ghost trains + Line reliability heatmap
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Anomalies & reliability</div>', unsafe_allow_html=True)
col5, col6 = st.columns(2)

with col5:
    st.markdown("**Ghost Train Rate by Line**")
    gt = data["ghost_trains"].sort_values("GHOST_PCT", ascending=True)
    fig = px.bar(
        gt, x="GHOST_PCT", y="LINE_ID", orientation="h",
        color="GHOST_PCT",
        color_continuous_scale=["#1a1e2a", "#f5a623", "#e8443a"],
        text="GHOST_PCT",
        labels={"GHOST_PCT": "Ghost %", "LINE_ID": ""},
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig.update_layout(**PLOT_THEME, height=320, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col6:
    st.markdown("**Line Reliability by Hour (Good Service %)**")
    rel = data["reliability"]
    if not rel.empty:
        pivot_rel = rel.pivot(index="LINE_ID", columns="HR", values="GOOD_SERVICE_PCT")
        fig = px.imshow(
            pivot_rel,
            color_continuous_scale=["#e8443a", "#f5a623", "#00c87a"],
            range_color=[0, 100],
            labels={"color": "%", "x": "Hour (UTC)", "y": ""},
        )
        fig.update_layout(**PLOT_THEME, height=320,
                          coloraxis_colorbar=dict(title="", tickfont=dict(size=9)))
        st.plotly_chart(fig, use_container_width=True)

# Active disruptions table
if not data["disruptions"].empty:
    with st.expander(f"⚠️ {len(data['disruptions'])} active disruption(s) in last hour"):
        st.dataframe(data["disruptions"], hide_index=True, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 4: Crowding + Lift disruptions
# Both use 10-min buckets to match the streaming cadence
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Crowding & accessibility — last 6 hours</div>', unsafe_allow_html=True)
col7, col8 = st.columns([3, 2])

with col7:
    tab1, tab2 = st.tabs(["Crowding (10-min)", "Lift disruptions (10-min)"])

    with tab1:
        cr = data["crowding_trend"].copy()
        if not cr.empty:
            cr["BUCKET"] = pd.to_datetime(cr["BUCKET"])
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=cr["BUCKET"], y=cr["AVG_CROWDING_PCT"],
                fill="tozeroy", name="Avg crowding %",
                line=dict(color="#3d8ef5", width=1.5),
                fillcolor="rgba(61,142,245,0.15)",
            ))
            fig.add_trace(go.Scatter(
                x=cr["BUCKET"], y=cr["STATIONS_REPORTING"],
                name="Stations reporting",
                yaxis="y2", mode="lines",
                line=dict(color="#00c87a", width=1, dash="dot"),
            ))
            apply_theme(fig)
            fig.update_layout(
                height=260,
                yaxis=dict(title="Avg % of baseline", gridcolor="#1a1e2a"),
                yaxis2=dict(title="Stations", overlaying="y", side="right"),
                legend=dict(orientation="h", y=1.15, font=dict(size=10)),
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        lt = data["lift_trend"].copy()
        if not lt.empty:
            lt["BUCKET"] = pd.to_datetime(lt["BUCKET"])
            fig = px.area(
                lt, x="BUCKET", y="DISRUPTIONS",
                labels={"DISRUPTIONS": "Active lift disruptions", "BUCKET": ""},
                color_discrete_sequence=["#f5a623"],
            )
            fig.update_traces(fillcolor="rgba(245,166,35,0.15)", line_width=1.5)
            fig.update_layout(**PLOT_THEME, height=260)
            st.plotly_chart(fig, use_container_width=True)

with col8:
    st.markdown("**Current lift outages (last hour)**")
    lf = data["lift_current"]
    if not lf.empty:
        fig = px.bar(
            lf, x="LIFT_COUNT", y="STATION_ID", orientation="h",
            color="LIFT_COUNT",
            color_continuous_scale=["#1e2330", "#f5a623", "#e8443a"],
            labels={"LIFT_COUNT": "Lifts out", "STATION_ID": ""},
        )
        fig.update_layout(**PLOT_THEME, height=260, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.markdown('<p style="color:#3a4055;padding-top:2rem;">No lift outages in last hour</p>',
                    unsafe_allow_html=True)

st.divider()

# ---------------------------------------------------------------------------
# Live ticker
# ---------------------------------------------------------------------------

st.markdown('<div class="section-label">Live arrival ticker — last 12 predictions resolved</div>', unsafe_allow_html=True)
ticker = data["ticker"].copy()
if not ticker.empty:
    ticker["STATUS"] = ticker["IS_ON_TIME"].map({1: "✅", 0: "⚠️"})
    ticker["ERROR_S"] = ticker["ERROR_S"].apply(lambda x: f"+{x}s" if x > 0 else f"{x}s")
    ticker = ticker.drop(columns=["IS_ON_TIME"]).rename(columns={
        "ARRIVAL_TIME": "Time",
        "LINE_ID":       "Line",
        "STATION_NAME":  "Station",
        "ERROR_S":       "Error",
        "SERVICE_PERIOD": "Period",
        "STATUS":        "",
    })
    st.dataframe(ticker, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown(
    '<p style="color:#2a2e3a;font-size:0.65rem;text-align:right;padding-top:1rem;">'
    'Spark → Snowflake · silver refreshes every 10 min · gold refreshes every hour'
    '</p>',
    unsafe_allow_html=True,
)