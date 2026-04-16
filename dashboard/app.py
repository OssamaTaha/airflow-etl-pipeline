"""
Airflow ETL Pipeline — Monitoring Dashboard (Demo Mode)
Deployed on Streamlit Community Cloud

Source: https://github.com/OssamaTaha/airflow-etl-pipeline
Full pipeline runs via Docker Compose locally — this dashboard shows
the monitoring interface with simulated data for portfolio demonstration.
"""
import streamlit as st
import pandas as pd
import random
from datetime import datetime, timedelta

st.set_page_config(
    page_title="ETL Pipeline Monitor",
    page_icon="📊",
    layout="wide",
)

# ── Header ──
st.title("Airflow ETL Pipeline Monitor")
st.caption("Multi-Source Data Orchestration with Apache Airflow")
st.caption("[GitHub Repository](https://github.com/OssamaTaha/airflow-etl-pipeline)")

st.info(
    "**Demo Mode** — This dashboard shows the monitoring interface for the "
    "Airflow ETL Pipeline project. The full stack runs locally via "
    "`docker compose up`. Data shown is simulated for portfolio demonstration.",
    icon="ℹ️",
)

st.divider()

# ── Generate realistic mock data ──
@st.cache_data
def generate_mock_data():
    now = datetime.utcnow()
    dag_ids = [
        "extract_exchange_rates",
        "extract_worldbank_indicators",
        "transform_and_load_warehouse",
        "data_pipeline_monitoring",
    ]
    schedules = {
        "extract_exchange_rates": "Daily 06:00 UTC",
        "extract_worldbank_indicators": "Weekly Sun 02:00 UTC",
        "transform_and_load_warehouse": "Daily 07:00 UTC",
        "data_pipeline_monitoring": "Every 6 hours",
    }

    # Pipeline runs (last 30 days)
    runs = []
    for dag_id in dag_ids:
        n_runs = 30 if "Daily" in schedules[dag_id] else (4 if "Weekly" in schedules[dag_id] else 120)
        for i in range(n_runs):
            t = now - timedelta(hours=i * (24 if "Daily" in schedules[dag_id] else (168 if "Weekly" in schedules[dag_id] else 6)))
            state = random.choices(["success", "success", "success", "success", "failed", "success"], weights=[70, 10, 5, 5, 5, 5])[0]
            duration = round(random.uniform(12, 180), 2) if state == "success" else round(random.uniform(5, 60), 2)
            runs.append({
                "dag_id": dag_id,
                "run_id": f"manual__{t.strftime('%Y-%m-%dT%H:%M')}",
                "state": state,
                "start_time": t,
                "end_time": t + timedelta(seconds=duration),
                "duration_seconds": duration,
            })
    runs_df = pd.DataFrame(runs)

    # Table stats
    tables = [
        ("staging", "exchange_rates", 4820, 1.2),
        ("staging", "worldbank_indicators", 2340, 0.8),
        ("staging", "api_users", 120, 0.1),
        ("production", "exchange_rates", 4510, 1.1),
        ("production", "country_indicators", 580, 0.4),
        ("production", "derived_metrics", 290, 0.2),
        ("monitoring", "pipeline_runs", 1860, 0.6),
        ("monitoring", "table_stats", 480, 0.2),
        ("monitoring", "quality_checks", 720, 0.3),
    ]
    table_df = pd.DataFrame(tables, columns=["schema", "table", "row_count", "size_mb"])
    table_df["full_name"] = table_df["schema"] + "." + table_df["table"]

    # Quality checks
    checks = []
    for i in range(20):
        t = now - timedelta(hours=i * 6)
        status = random.choices(["PASS", "PASS", "PASS", "WARN"], weights=[80, 10, 5, 5])[0]
        checks.append({
            "check_name": f"pipeline_health_check_{i}",
            "table_name": random.choice(table_df["full_name"].tolist()),
            "status": status,
            "message": "All DAGs healthy" if status == "PASS" else "High table growth rate",
            "checked_at": t,
        })
    checks_df = pd.DataFrame(checks)

    # Exchange rates (latest)
    currencies = ["EUR", "GBP", "EGP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "BRL", "SAR", "AED"]
    rates = {c: round(random.uniform(0.5, 160), 4) for c in currencies}
    rates_df = pd.DataFrame(
        [(b, t, r) for b in ["USD", "EUR"] for t, r in rates.items()],
        columns=["base_currency", "target_currency", "rate"],
    )

    return runs_df, table_df, checks_df, rates_df, schedules


runs_df, table_df, checks_df, rates_df, schedules = generate_mock_data()

# ── Sidebar ──
page = st.sidebar.radio("Navigation", ["Overview", "DAG Runs", "Tables", "Data Quality", "Exchange Rates"])

# ── Overview ──
if page == "Overview":
    st.header("Pipeline Overview")

    col1, col2, col3, col4 = st.columns(4)
    total = len(runs_df)
    success = len(runs_df[runs_df["state"] == "success"])
    col1.metric("Total DAG Runs", total)
    col2.metric("Successful", success)
    col3.metric("Success Rate", f"{success/total*100:.1f}%")
    col4.metric("Active DAGs", 4)

    st.divider()

    # DAG summary
    st.subheader("DAGs")
    for dag_id, schedule in schedules.items():
        dag_runs = runs_df[runs_df["dag_id"] == dag_id]
        last = dag_runs.iloc[0]["start_time"].strftime("%Y-%m-%d %H:%M UTC")
        success_pct = len(dag_runs[dag_runs["state"] == "success"]) / len(dag_runs) * 100
        avg_dur = dag_runs["duration_seconds"].mean()
        icon = "✅" if success_pct > 90 else "⚠️"
        st.markdown(
            f"{icon} **{dag_id}** — {schedule} | "
            f"Last: {last} | Avg: {avg_dur:.1f}s | Success: {success_pct:.0f}%"
        )

    st.divider()
    st.subheader("Recent Quality Checks")
    st.dataframe(checks_df.head(10), use_container_width=True, hide_index=True)

# ── DAG Runs ──
elif page == "DAG Runs":
    st.header("DAG Run History")

    selected_dag = st.selectbox("Filter by DAG", ["All"] + list(schedules.keys()))
    filtered = runs_df if selected_dag == "All" else runs_df[runs_df["dag_id"] == selected_dag]

    st.dataframe(
        filtered[["dag_id", "state", "start_time", "duration_seconds"]].head(100),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Run Durations (Last 30)")
    chart_data = filtered.head(30).copy()
    chart_data["label"] = chart_data["start_time"].dt.strftime("%m/%d %H:%M")
    st.bar_chart(chart_data.set_index("label")["duration_seconds"])

    st.subheader("Runs by State")
    state_counts = filtered["state"].value_counts()
    st.bar_chart(state_counts)

# ── Tables ──
elif page == "Tables":
    st.header("Table Statistics")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Row Counts")
        st.dataframe(
            table_df[["full_name", "row_count"]].sort_values("row_count", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
    with col2:
        st.subheader("Table Sizes (MB)")
        st.dataframe(
            table_df[["full_name", "size_mb"]].sort_values("size_mb", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Rows by Schema")
    schema_totals = table_df.groupby("schema")["row_count"].sum()
    st.bar_chart(schema_totals)

# ── Data Quality ──
elif page == "Data Quality":
    st.header("Data Quality Checks")

    statuses = checks_df["status"].unique()
    selected = st.multiselect("Filter by Status", list(statuses), default=list(statuses))
    filtered = checks_df[checks_df["status"].isin(selected)]

    def color_status(val):
        return f"color: {'green' if val == 'PASS' else 'orange' if val == 'WARN' else 'red'}"

    st.dataframe(
        filtered.style.map(color_status, subset=["status"]),
        use_container_width=True,
        hide_index=True,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Checks", len(checks_df))
    c2.metric("Passed", len(checks_df[checks_df["status"] == "PASS"]))
    c3.metric("Warnings", len(checks_df[checks_df["status"] == "WARN"]))

# ── Exchange Rates ──
elif page == "Exchange Rates":
    st.header("Latest Exchange Rates")

    base = st.selectbox("Base Currency", rates_df["base_currency"].unique())
    filtered = rates_df[rates_df["base_currency"] == base]

    col1, col2 = st.columns([1, 1])
    with col1:
        st.dataframe(filtered, use_container_width=True, hide_index=True)
    with col2:
        st.bar_chart(filtered.set_index("target_currency")["rate"])

# ── Footer ──
st.divider()
st.caption(
    "Built by Ossama Taha | "
    "[GitHub](https://github.com/OssamaTaha) | "
    "Tech: Apache Airflow 2.9, PostgreSQL 15, Docker, Python, Streamlit"
)
