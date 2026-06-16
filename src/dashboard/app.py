"""
Weather Data Warehouse — Streamlit Dashboard.
Sections: World Map · 7-Day Trends · Country Rankings · Extremes · KPI Cards
"""

import logging
import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="Weather DWH",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ──────────────────────────────────────────────────────────────────

@st.cache_resource
def _get_engine():
    from src.storage.postgres import get_engine
    return get_engine()


@st.cache_data(ttl=300)
def load_latest() -> pd.DataFrame:
    """Load latest observation per city (cached 5 min)."""
    engine = _get_engine()
    return pd.read_sql(
        """
        SELECT city_id, city_name, country, latitude, longitude,
               observed_at, temp_celsius, feels_like_celsius,
               humidity_pct, wind_speed_ms, cloud_cover_pct,
               visibility_m, is_daytime, dq_score
        FROM warehouse.v_latest_per_city
        ORDER BY city_name
        """,
        engine,
    )


@st.cache_data(ttl=300)
def load_7day_trends(city_id: int = None) -> pd.DataFrame:
    """Load 7-day daily trends (cached 5 min)."""
    engine = _get_engine()
    if city_id:
        return pd.read_sql(
            """
            SELECT city_id, city_name, country, full_date AS obs_date,
                   avg_temp_celsius, min_temp_celsius, max_temp_celsius,
                   avg_humidity_pct, avg_wind_speed_ms, dominant_condition,
                   prev_day_temp_celsius, temp_delta_celsius
            FROM api.mv_city_7day_trends
            WHERE city_id = %(city_id)s
            ORDER BY full_date
            """,
            engine,
            params={"city_id": city_id},
        )
    return pd.read_sql(
        """
        SELECT city_id, city_name, country, full_date AS obs_date,
               avg_temp_celsius, min_temp_celsius, max_temp_celsius,
               avg_humidity_pct, avg_wind_speed_ms, dominant_condition
        FROM api.mv_city_7day_trends
        ORDER BY full_date, city_name
        """,
        engine,
    )


@st.cache_data(ttl=300)
def load_country_snapshot() -> pd.DataFrame:
    """Load country-level snapshot (cached 5 min)."""
    engine = _get_engine()
    return pd.read_sql(
        "SELECT * FROM api.mv_country_snapshot ORDER BY avg_temp_celsius DESC",
        engine,
    )


@st.cache_data(ttl=300)
def load_extremes() -> pd.DataFrame:
    """Load extreme events last 30 days (cached 5 min)."""
    engine = _get_engine()
    return pd.read_sql(
        "SELECT * FROM warehouse.v_recent_extremes ORDER BY observed_at DESC LIMIT 100",
        engine,
    )


# ── Layout ───────────────────────────────────────────────────────────────────

st.title("Weather Data Warehouse — Live Dashboard")

# Sidebar
with st.sidebar:
    st.header("Filters")
    refresh = st.button("Refresh Data")
    if refresh:
        st.cache_data.clear()
        st.rerun()

try:
    df_latest = load_latest()
    df_country = load_country_snapshot()
    df_extremes = load_extremes()
    data_ok = True
except Exception as exc:
    st.error(f"Database connection failed: {exc}")
    st.info("Make sure your .env is configured and the pipeline has run at least once.")
    data_ok = False

if data_ok:
    # ── KPI Cards ──────────────────────────────────────────────────────────
    from src.dashboard.components.metrics import global_kpi_row
    global_kpi_row(df_latest)

    st.divider()

    # ── World Map ──────────────────────────────────────────────────────────
    from src.dashboard.components.charts import world_map
    st.plotly_chart(world_map(df_latest), use_container_width=True)

    st.divider()

    # ── 7-Day Trends ───────────────────────────────────────────────────────
    from src.dashboard.components.charts import temperature_trend
    st.subheader("7-Day City Temperature Trends")

    if not df_latest.empty:
        city_options = dict(
            zip(df_latest["city_name"] + " (" + df_latest["country"] + ")",
                df_latest["city_id"])
        )
        selected_city_label = st.selectbox("Select city", options=list(city_options.keys()))
        selected_city_id = city_options[selected_city_label]
        selected_city_name = selected_city_label.split(" (")[0]

        df_trend = load_7day_trends(city_id=selected_city_id)
        if not df_trend.empty:
            st.plotly_chart(
                temperature_trend(df_trend, selected_city_name),
                use_container_width=True,
            )
        else:
            st.info("No trend data available yet for this city.")
    else:
        st.info("No city data available.")

    st.divider()

    # ── Country Rankings ───────────────────────────────────────────────────
    from src.dashboard.components.charts import country_ranking_bar
    st.subheader("Country Rankings — 24h Average Temperature")
    if not df_country.empty:
        st.plotly_chart(country_ranking_bar(df_country), use_container_width=True)
    else:
        st.info("Country snapshot not yet available.")

    st.divider()

    # ── Extreme Events ─────────────────────────────────────────────────────
    from src.dashboard.components.charts import extremes_table
    st.subheader("Extreme Weather Events — Last 30 Days")
    if not df_extremes.empty:
        st.plotly_chart(extremes_table(df_extremes), use_container_width=True)
    else:
        st.success("No extreme events in the last 30 days.")

    # ── Raw Data Expander ──────────────────────────────────────────────────
    with st.expander("Raw: Latest Observations"):
        st.dataframe(df_latest, use_container_width=True)
