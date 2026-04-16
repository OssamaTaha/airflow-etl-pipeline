"""
Airflow ETL Pipeline Monitoring Dashboard
Run with: streamlit run dashboard/app.py --server.port 8501
"""
import os
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="ETL Pipeline Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Airflow ETL Pipeline — Monitoring Dashboard")
st.caption(f"Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")

# Database connection
DB_URL = os.environ.get(
    "AIRFLOW_CONN_POSTGRES_TARGET",
    "postgresql://target_user:target_pass@localhost:5432/etl_target",
)


def run_query(query: str, params=None) -> pd.DataFrame:
    """Execute a query and return a DataFrame."""
    try:
        conn = psycopg2.connect(DB_URL)
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# --- Sidebar ---
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "DAG Runs", "Tables", "Data Quality", "Exchange Rates"])

# --- Overview Page ---
if page == "Overview":
    st.header("Pipeline Overview")

    col1, col2, col3, col4 = st.columns(4)

    # Total DAG runs
    dag_stats = run_query(
        "SELECT state, COUNT(*) as count FROM monitoring.pipeline_runs GROUP BY state"
    )
    if not dag_stats.empty:
        total_runs = dag_stats["count"].sum()
        success_count = dag_stats[dag_stats["state"] == "success"]["count"].sum() if "success" in dag_stats["state"].values else 0
        col1.metric("Total DAG Runs", total_runs)
        col2.metric("Successful", success_count)
        col3.metric("Success Rate", f"{(success_count/total_runs*100):.1f}%" if total_runs > 0 else "N/A")

    # Database size
    db_size = run_query("SELECT pg_size_pretty(pg_database_size('etl_target')) as size")
    if not db_size.empty:
        col4.metric("DB Size", db_size.iloc[0]["size"])

    st.divider()

    # Recent quality checks
    st.subheader("Recent Quality Checks")
    checks = run_query(
        """
        SELECT check_name, table_name, status, message, checked_at
        FROM monitoring.quality_checks
        ORDER BY checked_at DESC LIMIT 10
        """
    )
    if not checks.empty:
        st.dataframe(checks, use_container_width=True)
    else:
        st.info("No quality checks recorded yet.")

# --- DAG Runs Page ---
elif page == "DAG Runs":
    st.header("DAG Run History")

    runs = run_query(
        """
        SELECT dag_id, state, start_time, end_time, duration_seconds
        FROM monitoring.pipeline_runs
        ORDER BY start_time DESC
        LIMIT 100
        """
    )

    if not runs.empty:
        # DAG filter
        dag_ids = runs["dag_id"].unique().tolist()
        selected_dag = st.selectbox("Filter by DAG", ["All"] + dag_ids)

        if selected_dag != "All":
            runs = runs[runs["dag_id"] == selected_dag]

        st.dataframe(runs, use_container_width=True)

        # Duration chart
        st.subheader("Run Durations")
        runs_chart = runs[runs["duration_seconds"].notna()].copy()
        if not runs_chart.empty:
            runs_chart["start_time"] = pd.to_datetime(runs_chart["start_time"])
            st.line_chart(runs_chart.set_index("start_time")["duration_seconds"])
    else:
        st.info("No DAG runs recorded yet. Run some DAGs first!")

# --- Tables Page ---
elif page == "Tables":
    st.header("Table Statistics")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Row Counts")
        rows = run_query(
            """
            SELECT schema_name, table_name, row_count, recorded_at
            FROM monitoring.table_stats
            WHERE recorded_at > NOW() - INTERVAL '24 hours'
            ORDER BY row_count DESC
            """
        )
        if not rows.empty:
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("No table stats recorded yet.")

    with col2:
        st.subheader("Table Sizes")
        sizes = run_query(
            """
            SELECT schemaname || '.' || tablename as table_name,
                   pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size
            FROM pg_tables
            WHERE schemaname IN ('staging', 'production', 'monitoring')
            ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
            """
        )
        if not sizes.empty:
            st.dataframe(sizes, use_container_width=True)

# --- Data Quality Page ---
elif page == "Data Quality":
    st.header("Data Quality Checks")

    checks = run_query(
        """
        SELECT check_name, table_name, status, message, checked_at
        FROM monitoring.quality_checks
        ORDER BY checked_at DESC
        LIMIT 50
        """
    )

    if not checks.empty:
        # Status filter
        statuses = checks["status"].unique().tolist()
        selected_status = st.multiselect("Filter by Status", statuses, default=statuses)
        filtered = checks[checks["status"].isin(selected_status)]

        # Color code
        def color_status(val):
            color = "green" if val == "PASS" else ("orange" if val == "WARN" else "red")
            return f"color: {color}"

        st.dataframe(filtered.style.applymap(color_status, subset=["status"]), use_container_width=True)

        # Summary
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Checks", len(checks))
        col2.metric("Passed", len(checks[checks["status"] == "PASS"]))
        col3.metric("Warnings", len(checks[checks["status"] == "WARN"]))
    else:
        st.info("No quality checks recorded yet.")

# --- Exchange Rates Page ---
elif page == "Exchange Rates":
    st.header("Latest Exchange Rates")

    rates = run_query(
        """
        SELECT base_currency, target_currency, rate, effective_date
        FROM production.exchange_rates
        WHERE effective_date = (SELECT MAX(effective_date) FROM production.exchange_rates)
        ORDER BY base_currency, target_currency
        """
    )

    if not rates.empty:
        col1, col2 = st.columns([2, 1])

        with col1:
            st.dataframe(rates, use_container_width=True)

        with col2:
            st.subheader("Rate Distribution")
            base = st.selectbox("Base Currency", rates["base_currency"].unique())
            filtered = rates[rates["base_currency"] == base]
            st.bar_chart(filtered.set_index("target_currency")["rate"])

        # Historical rates
        st.subheader("Rate History (Last 30 Days)")
        history = run_query(
            """
            SELECT base_currency, target_currency, rate, effective_date
            FROM production.exchange_rates
            WHERE effective_date > NOW() - INTERVAL '30 days'
            ORDER BY effective_date
            """
        )
        if not history.empty:
            pair = st.selectbox(
                "Currency Pair",
                [f"{r['base_currency']}/{r['target_currency']}" for _, r in rates.iterrows()],
            )
            base_curr, target_curr = pair.split("/")
            pair_data = history[
                (history["base_currency"] == base_curr) &
                (history["target_currency"] == target_curr)
            ]
            if not pair_data.empty:
                st.line_chart(pair_data.set_index("effective_date")["rate"])
    else:
        st.info("No exchange rate data in production yet.")
