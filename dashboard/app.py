"""Step 7 - Streamlit analytics dashboard.   streamlit run dashboard/app.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st  # noqa: E402

from src.analytics import run_query  # noqa: E402
from src.db import get_engine  # noqa: E402

st.set_page_config(page_title="Streaming Analytics", page_icon="🎬", layout="wide")
st.title("🎬 Media Streaming Analytics")
st.caption("Synthetic data  •  PostgreSQL  •  PySpark pipeline")


@st.cache_resource
def engine():
    return get_engine()


@st.cache_data(ttl=60)
def q(name: str):
    return run_query(engine(), name)


try:
    kpi = q("kpis").iloc[0]
except Exception as exc:  # noqa: BLE001
    st.error("Could not read from PostgreSQL. Is it running and has the pipeline been run?")
    st.code(str(exc))
    st.stop()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total events", f"{int(kpi.total_events):,}")
c2.metric("Unique viewers", f"{int(kpi.unique_viewers):,}")
c3.metric("Viewing sessions", f"{int(kpi.total_sessions):,}")
c4.metric("Completion rate", f"{kpi.completion_rate_pct}%")
c5.metric("Avg watch time", f"{kpi.avg_watch_minutes} min")

tab_content, tab_trend, tab_audience, tab_dq = st.tabs(
    ["Content", "Trends", "Audience", "Data quality"])

with tab_content:
    left, right = st.columns(2)
    top = q("top_content_by_views")
    left.subheader("Top 10 titles by views")
    left.bar_chart(top.set_index("content_title")["views"])
    right.subheader("Popular genres")
    right.bar_chart(q("popular_genres").set_index("genre")["views"])
    st.dataframe(top, use_container_width=True, hide_index=True)

with tab_trend:
    trend = q("viewing_trend")
    st.subheader("Events per day")
    st.line_chart(trend.set_index("event_date")["total_events"])
    st.subheader("Daily active viewers")
    st.line_chart(q("daily_active_viewers").set_index("event_date")["active_viewers"])
    st.subheader("Watch hours per day")
    st.line_chart(trend.set_index("event_date")["watch_hours"])

with tab_audience:
    a, b = st.columns(2)
    a.subheader("Activity by region")
    a.bar_chart(q("activity_by_region").set_index("region")["total_events"])
    b.subheader("Device distribution")
    b.bar_chart(q("device_distribution").set_index("device_type")["total_events"])
    st.subheader("Events by hour of day")
    st.bar_chart(q("popular_hours").set_index("event_hour")[["total_events", "play_events"]])
    st.subheader("Session outcomes")
    st.dataframe(q("completion_rate"), use_container_width=True, hide_index=True)

with tab_dq:
    dq = q("dq_latest_run")
    st.subheader("Latest validation run")
    if dq.empty:
        st.info("No data-quality logs yet.")
    else:
        overall = dq[dq.check_name == "OVERALL_REJECTED_ROWS"].iloc[0]
        d1, d2, d3 = st.columns(3)
        d1.metric("Records received", f"{int(overall.records_total):,}")
        d2.metric("Rejected", f"{int(overall.records_failed):,}")
        d3.metric("Rejection rate", f"{overall.failed_pct}%")
        st.bar_chart(dq[~dq.check_name.isin(["OVERALL_REJECTED_ROWS"])].set_index("check_name")["records_failed"])
        st.dataframe(dq, use_container_width=True, hide_index=True)
    st.subheader("Rejected rows by reason")
    st.dataframe(q("rejection_reasons_latest_run"), use_container_width=True, hide_index=True)
