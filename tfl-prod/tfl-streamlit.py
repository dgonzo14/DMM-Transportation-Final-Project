import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import pandas as pd

# Set page config
st.set_page_config(layout="wide", page_title="TfL Performance Command Center")

session = get_active_session()

st.title("🚇 TfL Metro Performance Command Center")
st.write("Comprehensive real-time analytics across the London Underground network.")

# --- 1. DATA LOADING SECTION ---
@st.cache_data(ttl=600)
def load_dashboard_data():
    # KPI 1: Reliability (Line Status)
    rel_df = session.sql("""
        SELECT LINE_NAME, HOUR(RECORD_TIMESTAMP) as HR,
        AVG(CASE WHEN SEVERITY_CODE = 10 THEN 100 ELSE 0 END) as RELIABILITY_PCT
        FROM SILVER_TFL_LINE_STATUS GROUP BY 1, 2 ORDER BY 2
    """).to_pandas()

    # KPI 2: Crowding
    crowd_df = session.sql("""
        SELECT OBSERVATION_TIME_UTC, CROWDING_PERCENTAGE
        FROM SILVER_TFL_CROWDING ORDER BY OBSERVATION_TIME_UTC ASC
    """).to_pandas()

    # KPI 3: Lifts
    lift_df = session.sql("""
        SELECT STATION_ID, COUNT(DISTINCT LIFT_ID) as DISRUPTION_COUNT
        FROM SILVER_TFL_LIFT_DISRUPTIONS GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).to_pandas()

    # KPI 4: Arrival Performance & Prediction Bias
    gold_df = session.sql("""
        SELECT 
            LINE_ID, 
            PREDICTION_ERROR_SECONDS,
            CASE 
                WHEN HOUR(initial_expected_ts) BETWEEN 7 AND 9 THEN 'AM Peak'
                WHEN HOUR(initial_expected_ts) BETWEEN 16 AND 18 THEN 'PM Peak'
                ELSE 'Off-Peak' 
            END as PERIOD
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
    """).to_pandas()

    # KPI 5: Jitter (Data Stability)
    j_df = session.sql("""
        SELECT STATION_NAME, LINE_ID, AVG(prediction_jitter) as AVG_JITTER, COUNT(*) as SAMPLE_SIZE
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        GROUP BY 1, 2 HAVING SAMPLE_SIZE > 5
        ORDER BY AVG_JITTER DESC LIMIT 15
    """).to_pandas()

    # KPI 6: Headway Analysis (Service Frequency)
    headway_df = session.sql("""
        WITH arrival_times AS (
            SELECT LINE_ID, STATION_NAME, actual_arrival_ts,
            LAG(actual_arrival_ts) OVER (PARTITION BY LINE_ID, STATION_NAME ORDER BY actual_arrival_ts ASC) as prev_arrival_ts
            FROM GOLD_TFL_ARRIVAL_PERFORMANCE
        )
        SELECT LINE_ID, 
        AVG(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)) / 60 as AVG_HEADWAY_MINS,
        STDDEV(DATEDIFF('second', prev_arrival_ts, actual_arrival_ts)) as HEADWAY_VARIANCE
        FROM arrival_times WHERE prev_arrival_ts IS NOT NULL
        GROUP BY 1
    """).to_pandas()

    # Update KPI 7 in load_dashboard_data()
    fresh_df = session.sql("""
        SELECT 
            DATEDIFF('second', MAX(DATA_CAPTURED_AT), CURRENT_TIMESTAMP()) as LATENCY
        FROM SILVER_TFL_ARRIVALS
    """).to_pandas()

    stats_df = session.sql("""
        SELECT 
            COUNT(DISTINCT PREDICTION_ID) as TOTAL_TRIPS,
            COUNT(DISTINCT STATION_ID) as ACTIVE_STATIONS,
            COUNT(DISTINCT VEHICLE_ID) as ACTIVE_TRAINS,
            -- We keep these subqueries pointed at GOLD because 
            -- 'Lag' and 'Busiest' need the performance math.
            (SELECT LINE_ID FROM GOLD_TFL_ARRIVAL_PERFORMANCE 
             GROUP BY 1 ORDER BY AVG(prediction_error_seconds) DESC LIMIT 1) as LAG_LINE,
            (SELECT STATION_NAME FROM GOLD_TFL_ARRIVAL_PERFORMANCE 
             GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1) as TOP_STATION
        FROM SILVER_TFL_ARRIVALS -- Changed from GOLD to SILVER
    """).to_pandas()

    return rel_df, crowd_df, lift_df, gold_df, j_df, headway_df, fresh_df, stats_df

# Load Data
df_rel, df_crowd, df_lift, df_gold, df_jitter, df_headway, df_fresh, df_stats = load_dashboard_data()

# --- 2. SYSTEM VITAL SIGNS ---
latency = df_fresh['LATENCY'].iloc[0]
if latency < 60:
    st.success(f"🟢 System Health: Data Fresh ({latency:.0f}s lag)")
elif latency < 180:
    st.warning(f"🟡 System Health: Data Lagging ({latency:.0f}s lag)")
else:
    st.error(f"🔴 System Health: Pipeline Stalled ({latency:.0f}s lag)")

# --- 3. NETWORK PULSE (Aggregate Metrics) ---
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.metric("Trips Tracked", f"{df_stats['TOTAL_TRIPS'].iloc[0]:,}")
with c2:
    st.metric("Active Stations", df_stats['ACTIVE_STATIONS'].iloc[0])
with c3:
    st.metric("Active Trains", df_stats['ACTIVE_TRAINS'].iloc[0])
with c4:
    st.metric("Laggiest Line", df_stats['LAG_LINE'].iloc[0].title())
with c5:
    st.metric("Top Station", df_stats['TOP_STATION'].iloc[0][:15] + "...")

st.divider()

# --- 4. RELIABILITY & FREQUENCY ---
col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Reliability Heatmap")
    pivot_df = df_rel.pivot(index='LINE_NAME', columns='HR', values='RELIABILITY_PCT')
    fig_rel = px.imshow(pivot_df, color_continuous_scale='RdYlGn', range_color=[0, 100])
    st.plotly_chart(fig_rel, use_container_width=True)

with col_b:
    st.subheader("Service Frequency (Headway)")
    fig_headway = px.bar(
        df_headway, x='LINE_ID', y='AVG_HEADWAY_MINS', color='HEADWAY_VARIANCE',
        title="Avg Min between Trains (Color = Bunching/Variance)",
        color_continuous_scale='YlOrRd'
    )
    st.plotly_chart(fig_headway, use_container_width=True)

st.divider()

# --- 5. PERFORMANCE BIAS & STABILITY ---
col_c, col_d = st.columns(2)
with col_c:
    st.subheader("The 'Peak Penalty': Prediction Accuracy")
    fig_peak = px.box(df_gold, x="PERIOD", y="PREDICTION_ERROR_SECONDS", color="LINE_ID",
                     category_orders={"PERIOD": ["AM Peak", "Off-Peak", "PM Peak"]})
    fig_peak.update_yaxes(range=[-300, 600], title="Error (Seconds)")
    st.plotly_chart(fig_peak, use_container_width=True)

with col_d:
    st.subheader("Data Stability: Highest Jitter")
    fig_jitter = px.bar(df_jitter.sort_values('AVG_JITTER', ascending=True),
                        x='AVG_JITTER', y='STATION_NAME', color='LINE_ID', orientation='h',
                        labels={'AVG_JITTER': 'Jitter (Sec)', 'STATION_NAME': ''})
    st.plotly_chart(fig_jitter, use_container_width=True)

st.divider()

# --- 6. CROWDING, ACCESS & TICKER ---
col_e, col_f = st.columns([2, 1])
with col_e:
    st.subheader("Crowding Trends & Lift Status")
    sub_c1, sub_c2 = st.columns(2)
    with sub_c1:
        df_crowd['OBSERVATION_TIME_UTC'] = pd.to_datetime(df_crowd['OBSERVATION_TIME_UTC'])
        df_hourly = df_crowd.set_index('OBSERVATION_TIME_UTC').resample('H').mean().reset_index()
        st.line_chart(df_hourly, x="OBSERVATION_TIME_UTC", y="CROWDING_PERCENTAGE")
    with sub_c2:
        st.bar_chart(df_lift, x="STATION_ID", y="DISRUPTION_COUNT", color="#ff4b4b")

with col_f:
    st.subheader("🕒 Live Arrival Ticker")
    recent = session.sql("""
        SELECT actual_arrival_ts::time as TIME, LINE_ID, STATION_NAME, prediction_error_seconds as DELAY
        FROM GOLD_TFL_ARRIVAL_PERFORMANCE ORDER BY actual_arrival_ts DESC LIMIT 8
    """).to_pandas()
    st.dataframe(recent, hide_index=True)