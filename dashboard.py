import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Ride-Sharing Dashboard", layout="wide")
st.title("🚕 Real-Time Ride-Sharing Trips Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5432/kafka_db"


@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


engine = get_engine(DATABASE_URL)


def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM trips"
    params: dict = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY pickup_time DESC LIMIT :limit"
    params["limit"] = limit

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Sidebar controls
status_options = ["All", "Requested", "Completed", "Cancelled"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider(
    "Update Interval (seconds)", min_value=2, max_value=20, value=5
)
limit_records = st.sidebar.number_input(
    "Number of records to load", min_value=50, max_value=5000, value=500, step=50
)

if st.sidebar.button("Refresh now"):
    st.rerun()

placeholder = st.empty()

while True:
    df_trips = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_trips.empty:
            st.warning("No trip records found yet. Waiting for data...")
            time.sleep(update_interval)
            continue

        # Timestamps to datetime
        if "pickup_time" in df_trips.columns:
            df_trips["pickup_time"] = pd.to_datetime(df_trips["pickup_time"])
        if "dropoff_time" in df_trips.columns:
            df_trips["dropoff_time"] = pd.to_datetime(df_trips["dropoff_time"])

        # Derive trip duration in minutes
        df_trips["duration_min"] = (
            (df_trips["dropoff_time"] - df_trips["pickup_time"])
            .dt.total_seconds()
            .div(60)
            .round(1)
        )

        # KPIs
        total_trips = len(df_trips)
        total_revenue = df_trips["fare"].sum()
        avg_fare = total_revenue / total_trips if total_trips > 0 else 0.0
        avg_distance = df_trips["distance_km"].mean() if total_trips > 0 else 0.0
        completed = len(df_trips[df_trips["status"] == "Completed"])
        completion_rate = (completed / total_trips * 100) if total_trips > 0 else 0.0

        st.subheader(f"Displaying {total_trips} trips (Filter: {selected_status})")

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Trips", total_trips)
        k2.metric("Total Revenue", f"${total_revenue:,.2f}")
        k3.metric("Average Fare", f"${avg_fare:,.2f}")
        k4.metric("Avg Distance (km)", f"{avg_distance:,.2f}")
        k5.metric("Completion Rate", f"{completion_rate:,.2f}%")

        st.markdown("### Raw Data (Top 10)")
        st.dataframe(df_trips.head(10), use_container_width=True)

        # Charts
        # 1. Trips per city
        trips_by_city = (
            df_trips.groupby("city")["trip_id"].count().reset_index(name="trip_count")
        )
        fig_trips_city = px.bar(
            trips_by_city,
            x="city",
            y="trip_count",
            title="Trips by City",
            labels={"trip_count": "Number of Trips", "city": "City"},
        )

        # 2. Revenue over time (per minute)
        ts = (
            df_trips.set_index("pickup_time")
            .resample("1min")["fare"]
            .sum()
            .reset_index()
        )
        fig_revenue_time = px.line(
            ts,
            x="pickup_time",
            y="fare",
            title="Revenue Over Time (per minute)",
            labels={"pickup_time": "Time", "fare": "Revenue"},
        )

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.plotly_chart(fig_trips_city, use_container_width=True)
        with chart_col2:
            st.plotly_chart(fig_revenue_time, use_container_width=True)

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)