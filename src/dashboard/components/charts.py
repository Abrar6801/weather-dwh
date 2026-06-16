"""Reusable Plotly chart components for the Streamlit dashboard."""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def world_map(df: pd.DataFrame) -> go.Figure:
    """
    Scatter geo map — one bubble per city coloured by temperature.
    df must have: city_name, country, latitude, longitude, temp_celsius, humidity_pct
    """
    fig = px.scatter_geo(
        df,
        lat="latitude",
        lon="longitude",
        color="temp_celsius",
        size="humidity_pct",
        hover_name="city_name",
        hover_data={
            "country": True,
            "temp_celsius": ":.1f",
            "humidity_pct": True,
            "latitude": False,
            "longitude": False,
        },
        color_continuous_scale="RdYlBu_r",
        title="Current Temperature by City",
        projection="natural earth",
    )
    fig.update_layout(
        geo=dict(showland=True, landcolor="lightgray", showocean=True, oceancolor="lightblue"),
        coloraxis_colorbar_title="Temp (°C)",
        margin={"r": 0, "t": 40, "l": 0, "b": 0},
    )
    return fig


def temperature_trend(df: pd.DataFrame, city_name: str) -> go.Figure:
    """
    Line chart of daily avg/min/max temperature for a single city.
    df must have: obs_date, avg_temp_celsius, min_temp_celsius, max_temp_celsius
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["obs_date"], y=df["avg_temp_celsius"],
        name="Avg", line=dict(color="#EF553B", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=df["obs_date"], y=df["max_temp_celsius"],
        name="Max", line=dict(color="#FF7F0E", dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=df["obs_date"], y=df["min_temp_celsius"],
        name="Min", line=dict(color="#1F77B4", dash="dot"),
    ))
    fig.update_layout(
        title=f"7-Day Temperature — {city_name}",
        xaxis_title="Date",
        yaxis_title="Temperature (°C)",
        legend=dict(orientation="h"),
        hovermode="x unified",
    )
    return fig


def country_ranking_bar(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart of countries ranked by average temperature.
    df must have: country, avg_temp_celsius
    """
    df_sorted = df.sort_values("avg_temp_celsius", ascending=True)
    fig = px.bar(
        df_sorted,
        x="avg_temp_celsius",
        y="country",
        orientation="h",
        color="avg_temp_celsius",
        color_continuous_scale="RdYlBu_r",
        labels={"avg_temp_celsius": "Avg Temp (°C)", "country": "Country"},
        title="Country Rankings by Average Temperature (24h)",
    )
    fig.update_layout(coloraxis_showscale=False, yaxis_title="")
    return fig


def extremes_table(df: pd.DataFrame) -> go.Figure:
    """
    Table visualization of extreme weather events.
    df must have: city_name, country, observed_at, event_type, temp_celsius, wind_speed_ms
    """
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=["City", "Country", "Observed At", "Event Type", "Temp (°C)", "Wind (m/s)"],
            fill_color="#2b2b2b",
            font=dict(color="white"),
            align="left",
        ),
        cells=dict(
            values=[
                df.get("city_name", []),
                df.get("country", []),
                df.get("observed_at", []),
                df.get("event_type", []),
                df.get("temp_celsius", []),
                df.get("wind_speed_ms", []),
            ],
            fill_color=[["#fff0f0" if et in ("extreme_heat", "storm_wind") else "white"
                         for et in df.get("event_type", [])]],
            align="left",
        ),
    )])
    fig.update_layout(title="Extreme Events — Last 30 Days", margin={"t": 40})
    return fig
