"""KPI card components for the Streamlit dashboard."""

import streamlit as st


def kpi_card(label: str, value: str, delta: str = None, help_text: str = None) -> None:
    """Render a single KPI metric card using st.metric."""
    st.metric(label=label, value=value, delta=delta, help=help_text)


def global_kpi_row(df_latest) -> None:
    """
    Render a row of 4 KPI cards from the latest observations DataFrame.
    Expected columns: temp_celsius, humidity_pct, wind_speed_ms, dq_score
    """
    import pandas as pd
    cols = st.columns(4)

    if df_latest is None or df_latest.empty:
        for col, label in zip(cols, ["Avg Temp", "Avg Humidity", "Avg Wind", "Avg DQ Score"]):
            with col:
                kpi_card(label, "N/A")
        return

    avg_temp = df_latest["temp_celsius"].mean()
    avg_humidity = df_latest["humidity_pct"].mean()
    avg_wind = df_latest["wind_speed_ms"].mean()
    avg_dq = df_latest["dq_score"].mean() if "dq_score" in df_latest.columns else None

    with cols[0]:
        kpi_card("Avg Temperature", f"{avg_temp:.1f} °C")
    with cols[1]:
        kpi_card("Avg Humidity", f"{avg_humidity:.0f}%")
    with cols[2]:
        kpi_card("Avg Wind Speed", f"{avg_wind:.1f} m/s")
    with cols[3]:
        kpi_card("Avg DQ Score", f"{avg_dq:.0f}" if avg_dq is not None else "N/A")
