import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

MONITORING_CATALOG = os.getenv("MONITORING_CATALOG", "monitoring_dev")
BRONZE_CATALOG     = os.getenv("BRONZE_CATALOG",     "bronze_dev")

# ── Page config — must be the first Streamlit call ────────────────────────────
st.set_page_config(
    page_title="Databricks Billing Usage",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── SQL building blocks ────────────────────────────────────────────────────────
_PRICE_JOIN = f"""
  LEFT JOIN {MONITORING_CATALOG}.system_billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)"""

_FX_JOIN = f"""
  LEFT JOIN (
    SELECT rate_date, usd_to_gbp,
      COALESCE(LEAD(rate_date) OVER (ORDER BY rate_date), DATE_ADD(rate_date, 365)) AS next_rate_date
    FROM {BRONZE_CATALOG}.raw_reference.fx_rates_usd_gbp
  ) fx ON u.usage_date >= fx.rate_date AND u.usage_date < fx.next_rate_date"""

_PIPELINE_JOIN = f"""
  LEFT JOIN {MONITORING_CATALOG}.system_billing.pipelines pl
    ON u.usage_metadata.dlt_pipeline_id = pl.pipeline_id"""

# cost in GBP for one DBU row
_COST = ("CAST(u.usage_quantity AS DOUBLE)"
         " * COALESCE(CAST(p.pricing.effective_list.default AS DOUBLE), 0)"
         " * COALESCE(fx.usd_to_gbp, 0.79)")

# ── Connection (shared across all reruns) ─────────────────────────────────────
@st.cache_resource
def _get_conn():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=300, show_spinner="Querying Databricks…")
def run(query: str) -> pd.DataFrame:
    conn = _get_conn()
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def safe_run(query: str) -> pd.DataFrame:
    """Like run() but returns an empty DataFrame instead of raising."""
    try:
        return run(query)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Fetching query profile…")
def _fetch_profile(statement_id: str) -> dict:
    cfg = Config()
    resp = requests.get(
        f"{cfg.host.rstrip('/')}/api/2.0/sql/history/{statement_id}/profile",
        headers={"Authorization": f"Bearer {cfg.token}"},
        timeout=15,
    )
    if resp.ok:
        return resp.json()
    return {"_error": f"HTTP {resp.status_code}", "_body": resp.text[:400]}


@st.cache_data(ttl=300, show_spinner="Loading query metrics…")
def _fetch_query_metrics(statement_id: str) -> dict:
    """Fetch optional columns that may not exist on all workspace tiers."""
    try:
        df = run(f"""
        SELECT
          COALESCE(read_rows,     0) AS read_rows,
          COALESCE(read_bytes,    0) AS read_bytes,
          COALESCE(produced_rows, 0) AS produced_rows,
          from_result_cache
        FROM {MONITORING_CATALOG}.system_billing.query_history
        WHERE statement_id = '{statement_id}'
        LIMIT 1
        """)
        if df.empty:
            return {}
        r = df.iloc[0]
        return {
            "read_rows":       int(r.read_rows),
            "read_bytes":      int(r.read_bytes),
            "produced_rows":   int(r.produced_rows),
            "from_result_cache": bool(r.from_result_cache),
        }
    except Exception:
        return {}


def _parse_profile(profile: dict) -> pd.DataFrame:
    nodes = (
        profile.get("nodes")
        or profile.get("plan_summary", {}).get("nodes")
        or []
    )
    if not nodes:
        return pd.DataFrame()
    rows = []
    for n in nodes:
        duration_s = (
            (n.get("time_ns") or 0) / 1e9
            or (n.get("execution_time_ms") or 0) / 1e3
            or (n.get("duration_ms") or 0) / 1e3
        )
        rows.append({
            "operator":  n.get("node_name") or n.get("name") or n.get("description") or "—",
            "duration_s": round(duration_s, 3),
            "rows_out":  int(n.get("rows_output") or n.get("num_output_rows") or n.get("output_rows") or 0),
            "size_mb":   round((n.get("size_output_bytes") or n.get("size_bytes") or 0) / 1e6, 2),
        })
    return pd.DataFrame(rows).sort_values("duration_s", ascending=False).reset_index(drop=True)


# ── Sidebar controls ──────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Controls")

    today = date.today()
    data_start = date(2025, 6, 1)

    start_date = st.date_input("From", data_start, min_value=data_start, max_value=today)
    end_date   = st.date_input("To",   today,       min_value=data_start, max_value=today)

    if start_date >= end_date:
        st.error("'From' must be before 'To'.")
        st.stop()

    st.divider()
    granularity = st.radio(
        "Granularity",
        ["Daily", "Weekly", "Monthly"],
        index=0,
        help="Controls the time axis on all trend charts.",
    )
    trunc = {"Daily": "DAY", "Weekly": "WEEK", "Monthly": "MONTH"}[granularity]

    st.divider()
    top_n = st.select_slider(
        "Top N Workloads", options=[5, 10, 15, 20, 25, 50], value=10
    )

    st.divider()
    show_forecast = st.toggle(
        "Show 30-day Forecast",
        value=False,
        help="Adds an ai_forecast() overlay to the trend chart. Takes ~20 s.",
    )

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        run.clear()
        st.rerun()

    st.caption(
        "**Source:** `system.billing.usage`  \n"
        "**FX:** Live ECB rates via `bronze_dev.raw_reference.fx_rates_usd_gbp`  \n"
        "**Prices:** Databricks list price · compute only"
    )

# ── Shared date filter ─────────────────────────────────────────────────────────
_date_filter = f"u.usage_date BETWEEN '{start_date}' AND '{end_date}'"

# ── Header ────────────────────────────────────────────────────────────────────
st.title("💰 Databricks Billing Usage")
st.caption(
    f"**{start_date} → {end_date}** · {granularity} · Top {top_n} workloads "
    f"· All costs in **£ GBP** (list price, compute only)"
)

# ── KPIs ──────────────────────────────────────────────────────────────────────
kpi = run(f"""
SELECT
  ROUND(SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END), 2)   AS total_cost_gbp,
  ROUND(SUM(CASE WHEN u.usage_unit='DBU' THEN CAST(u.usage_quantity AS DOUBLE) ELSE 0 END), 2) AS total_dbus,
  COUNT(DISTINCT u.usage_date) AS active_days,
  CONCAT(DATE_FORMAT(MIN(u.usage_date),'MMM yyyy'),' to ',DATE_FORMAT(MAX(u.usage_date),'MMM yyyy')) AS coverage
FROM {MONITORING_CATALOG}.system_billing.usage u
{_PRICE_JOIN}
{_FX_JOIN}
WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Cost (£)",  f"£{float(kpi.total_cost_gbp):,.2f}",
          help="Compute list price · excludes storage & cloud infrastructure")
c2.metric("Total DBUs",       f"{float(kpi.total_dbus):,.2f}",
          help="Databricks Units consumed across all workloads")
c3.metric("Active Days",      int(kpi.active_days),
          help="Calendar days with any logged usage (incl. background activity)")
c4.metric("Coverage",         kpi.coverage,
          help="Date range of usage data in the selected period")

st.divider()

# ── Usage over time ────────────────────────────────────────────────────────────
st.subheader(f"Total Usage Over Time ({granularity})")

time_df = run(f"""
SELECT
  DATE_TRUNC('{trunc}', u.usage_date)   AS period,
  billing_origin_product,
  ROUND(SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END), 4) AS cost_gbp
FROM {MONITORING_CATALOG}.system_billing.usage u
{_PRICE_JOIN}
{_FX_JOIN}
WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
GROUP BY 1, 2
ORDER BY 1
""")
time_df["cost_gbp"] = time_df["cost_gbp"].astype(float)
total_time = time_df.groupby("period", as_index=False)["cost_gbp"].sum()

if show_forecast:
    fc_freq_map     = {"Daily": "D", "Weekly": "W", "Monthly": "MS"}
    fc_horizon_days = {"Daily": 30,  "Weekly": 84,  "Monthly": 91}
    fc_freq    = fc_freq_map[granularity]
    fc_horizon = (today + timedelta(days=fc_horizon_days[granularity])).strftime("%Y-%m-%d")

    fc_df = run(f"""
    SELECT ds AS period, GREATEST(y_forecast, 0) AS cost_gbp
    FROM ai_forecast(
      TABLE(
        SELECT DATE_TRUNC('{trunc}', u.usage_date) AS ds,
          SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END) AS y
        FROM {MONITORING_CATALOG}.system_billing.usage u
        {_PRICE_JOIN}
        {_FX_JOIN}
        WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
        GROUP BY DATE_TRUNC('{trunc}', u.usage_date)
        ORDER BY ds
      ),
      horizon    => '{fc_horizon}',
      time_col   => 'ds',
      value_col  => 'y',
      frequency  => '{fc_freq}'
    )
    """)
    fc_df["cost_gbp"] = fc_df["cost_gbp"].astype(float)

    fig_time = go.Figure()
    fig_time.add_trace(go.Scatter(
        x=total_time["period"], y=total_time["cost_gbp"],
        name="Actual", mode="lines",
        fill="tozeroy", fillcolor="rgba(27, 118, 194, 0.3)",
        line=dict(color="#1B76C2", width=2),
    ))
    fig_time.add_trace(go.Scatter(
        x=fc_df["period"], y=fc_df["cost_gbp"],
        name="Forecast", mode="lines",
        fill="tozeroy", fillcolor="rgba(255, 54, 33, 0.15)",
        line=dict(color="#FF3621", width=2),
    ))
else:
    fig_time = px.area(
        total_time, x="period", y="cost_gbp",
        labels={"period": "", "cost_gbp": "Cost (£)"},
        color_discrete_sequence=["#1B76C2"],
    )

fig_time.update_layout(
    height=320, margin=dict(l=0, r=0, t=10, b=0),
    yaxis_tickprefix="£", hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_time, use_container_width=True)

# ── Product breakdown + avg cost ───────────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("Usage by Product (£)")

    prod_df = run(f"""
    SELECT billing_origin_product,
      ROUND(SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END), 2) AS cost_gbp
    FROM {MONITORING_CATALOG}.system_billing.usage u
    {_PRICE_JOIN}
    {_FX_JOIN}
    WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
    GROUP BY 1
    HAVING cost_gbp > 0
    ORDER BY 2 DESC
    """)
    prod_df["cost_gbp"] = prod_df["cost_gbp"].astype(float)

    fig_prod = px.bar(
        prod_df, x="cost_gbp", y="billing_origin_product",
        orientation="h",
        labels={"cost_gbp": "Cost (£)", "billing_origin_product": ""},
        color="cost_gbp", color_continuous_scale="Blues_r",
        text=prod_df["cost_gbp"].apply(lambda v: f"£{v:.2f}"),
    )
    fig_prod.update_traces(textposition="outside")
    fig_prod.update_layout(
        height=300, coloraxis_showscale=False,
        margin=dict(l=0, r=60, t=10, b=0),
        xaxis_tickprefix="£",
    )
    st.plotly_chart(fig_prod, use_container_width=True)

with col_right:
    st.subheader("Avg Cost per Period (£)")

    avg_df = run(f"""
    WITH base AS (
      SELECT u.usage_date,
        SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END) AS day_cost
      FROM {MONITORING_CATALOG}.system_billing.usage u
      {_PRICE_JOIN}
      {_FX_JOIN}
      WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
      GROUP BY u.usage_date
    )
    SELECT 'Daily'   AS period, ROUND(AVG(day_cost), 2)                                              AS avg_cost_gbp FROM base
    UNION ALL
    SELECT 'Weekly',  ROUND(AVG(wk_cost), 2) FROM (SELECT SUM(day_cost) AS wk_cost FROM base GROUP BY DATE_TRUNC('WEEK',  usage_date))
    UNION ALL
    SELECT 'Monthly', ROUND(AVG(mo_cost), 2) FROM (SELECT SUM(day_cost) AS mo_cost FROM base GROUP BY DATE_TRUNC('MONTH', usage_date))
    """)
    avg_df["avg_cost_gbp"] = avg_df["avg_cost_gbp"].astype(float).apply(lambda v: f"£{v:,.2f}")
    avg_df.columns = ["Period", "Avg Cost (£)"]
    st.dataframe(avg_df, hide_index=True, use_container_width=True, height=175)

    st.caption(
        "ℹ️ **Daily** = avg cost per calendar day  \n"
        "**Weekly** = avg cost per week  \n"
        "**Monthly** = avg cost per month"
    )

# ── Stacked bar by product over time ──────────────────────────────────────────
st.divider()
st.subheader(f"Usage by Product Over Time ({granularity})")

fig_stack = px.bar(
    time_df, x="period", y="cost_gbp", color="billing_origin_product",
    labels={"period": "", "cost_gbp": "Cost (£)", "billing_origin_product": "Product"},
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_stack.update_layout(
    height=380, barmode="stack",
    margin=dict(l=0, r=0, t=10, b=0),
    yaxis_tickprefix="£",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_stack, use_container_width=True)

# ── Top N workloads ────────────────────────────────────────────────────────────
st.divider()
st.subheader(f"Top {top_n} Workloads by DBU")

wl_df = run(f"""
SELECT
  ROW_NUMBER() OVER (ORDER BY SUM(CAST(u.usage_quantity AS DOUBLE)) DESC) AS rank,
  COALESCE(u.usage_metadata.job_name, u.usage_metadata.notebook_path, pl.name,
           u.usage_metadata.dlt_pipeline_id)                               AS workload,
  u.billing_origin_product                                                  AS product,
  ROUND(SUM(CAST(u.usage_quantity AS DOUBLE)), 4)                          AS total_dbus,
  ROUND(SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END), 2)    AS cost_gbp
FROM {MONITORING_CATALOG}.system_billing.usage u
{_PIPELINE_JOIN}
{_PRICE_JOIN}
{_FX_JOIN}
WHERE u.record_type = 'ORIGINAL'
  AND u.usage_unit = 'DBU'
  AND {_date_filter}
  AND COALESCE(u.usage_metadata.job_name, u.usage_metadata.notebook_path,
               u.usage_metadata.dlt_pipeline_id) IS NOT NULL
GROUP BY workload, product
ORDER BY total_dbus DESC
LIMIT {top_n}
""")
wl_df["cost_gbp"]   = wl_df["cost_gbp"].astype(float)
wl_df["total_dbus"] = wl_df["total_dbus"].astype(float)

fig_wl = px.bar(
    wl_df, x="total_dbus", y="workload",
    orientation="h",
    color="product",
    labels={"total_dbus": "Total DBUs", "workload": "", "product": "Product"},
    color_discrete_sequence=px.colors.qualitative.Set2,
    text=wl_df["cost_gbp"].apply(lambda v: f"£{v:.2f}"),
)
fig_wl.update_traces(textposition="outside")
fig_wl.update_layout(
    height=max(300, top_n * 32),
    margin=dict(l=0, r=80, t=10, b=0),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_wl, use_container_width=True)

st.dataframe(
    wl_df.rename(columns={
        "rank": "#", "workload": "Workload", "product": "Product",
        "total_dbus": "Total DBUs", "cost_gbp": "Est. Cost (£)",
    }),
    hide_index=True,
    use_container_width=True,
    height=min(450, (top_n + 1) * 40),
)

# ── Performance Drill-down ─────────────────────────────────────────────────────
st.divider()
st.header("⏱️ Performance Drill-down")
st.caption("Query execution times, DLT pipeline durations, and job task breakdown.")

perf_tab1, perf_tab2, perf_tab3 = st.tabs(
    ["SQL Query Times", "DLT Pipeline Durations", "Job Task Breakdown"]
)

# ── Tab 1: SQL Query Times ─────────────────────────────────────────────────────
with perf_tab1:
    col_a, col_b = st.columns([3, 2])

    with col_a:
        st.subheader(f"Top {top_n} Slowest SQL Queries")
        slow_df = safe_run(f"""
        SELECT
          statement_id,
          ROUND(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time), 1) AS duration_s,
          executed_by,
          statement_text,
          LEFT(statement_text, 120)                                        AS query_preview
        FROM {MONITORING_CATALOG}.system_billing.query_history
        WHERE DATE(start_time) BETWEEN '{start_date}' AND '{end_date}'
          AND error_message IS NULL
          AND end_time IS NOT NULL
        ORDER BY duration_s DESC
        LIMIT {top_n}
        """)
        if slow_df.empty:
            st.info("No query history data found. Run **sync_system_billing_job** to populate the `query_history` table, then refresh.")
        else:
            slow_df["duration_s"] = slow_df["duration_s"].astype(float)

            fig_slow = px.bar(
                slow_df, x="duration_s", y="query_preview",
                orientation="h",
                color="executed_by",
                labels={"duration_s": "Duration (s)", "query_preview": "",
                        "executed_by": "Run by"},
                color_discrete_sequence=px.colors.qualitative.Set2,
                text=slow_df["duration_s"].apply(lambda v: f"{v:.1f}s"),
            )
            fig_slow.update_traces(textposition="outside")
            fig_slow.update_layout(
                height=max(300, top_n * 36),
                margin=dict(l=0, r=60, t=10, b=0),
                xaxis_title="Duration (s)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_slow, use_container_width=True)

    with col_b:
        st.subheader("Query Detail")
        if not slow_df.empty:
            detail = slow_df[["duration_s", "executed_by", "query_preview"]].rename(columns={
                "duration_s": "Duration (s)", "executed_by": "Run by",
                "query_preview": "Query (preview)",
            })
            st.dataframe(detail, hide_index=True, use_container_width=True, height=400)

    # ── Query Profile ──────────────────────────────────────────────────────────
    if not slow_df.empty:
        st.divider()
        st.subheader("🔍 Query Profile")

        options = {
            f"{i+1}. {row.duration_s:.1f}s — {row.query_preview[:80]}": i
            for i, row in slow_df.iterrows()
        }
        selected_label = st.selectbox("Select a query to profile", list(options.keys()), index=0)
        row = slow_df.iloc[options[selected_label]]

        metrics = _fetch_query_metrics(str(row.statement_id))
        has_metrics = bool(metrics)

        read_mb    = metrics.get("read_bytes", 0) / 1e6
        filter_pct = metrics.get("produced_rows", 0) / max(metrics.get("read_rows", 1), 1) * 100
        cached     = metrics.get("from_result_cache", False)

        # Always-available metrics from query history
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Duration",      f"{float(row.duration_s):.1f}s")
        k2.metric("Data Scanned",  f"{read_mb:,.1f} MB" if has_metrics else "—")
        k3.metric("Rows Read",     f"{metrics.get('read_rows', 0):,}" if has_metrics else "—")
        k4.metric("Rows Returned", f"{metrics.get('produced_rows', 0):,}" if has_metrics else "—")
        k5.metric("Result Cached", ("Yes ✅" if cached else "No") if has_metrics else "—")

        # ── Performance Tip ───────────────────────────────────────────────────
        if not has_metrics:
            with st.container(border=True):
                st.markdown("#### ℹ️ Metrics not available")
                st.markdown(
                    "Extended metrics (`read_bytes`, `read_rows`, `produced_rows`) are not present "
                    "in this workspace's `system.query.history`. Performance tips require these columns."
                )
        elif cached:
            with st.container(border=True):
                st.markdown("#### ✅ Result Cache Hit")
                st.markdown(
                    "**Reason:** This query was served from the result cache.  \n"
                    "Actual compute cost was near zero — no action needed."
                )
        elif read_mb > 500 and filter_pct < 10:
            with st.container(border=True):
                st.markdown("#### 💡 Performance Tip")
                st.markdown(
                    f"**Reason:** Scanned **{read_mb:,.0f} MB** but only **{filter_pct:.1f}%** "
                    f"of rows passed the filter.  \n"
                    "Most of that data was read and then thrown away."
                )
                st.markdown(
                    "🔧 **Fix:** Z-ORDER the table on the columns used in your WHERE clause. "
                    "This lets Databricks skip entire files that can't match the filter."
                )
                st.code(
                    "OPTIMIZE <catalog>.<schema>.<table>\n"
                    "ZORDER BY (<your_filter_column>);",
                    language="sql",
                )
        elif read_mb > 1000:
            with st.container(border=True):
                st.markdown("#### 💡 Performance Tip")
                st.markdown(
                    f"**Reason:** Large scan — **{read_mb:,.0f} MB** read.  \n"
                    "This is often caused by many small Delta files accumulating over time."
                )
                st.markdown(
                    "🔧 **Fix:** Run OPTIMIZE to compact them into fewer, larger files."
                )
                st.code(
                    "OPTIMIZE <catalog>.<schema>.<table>;",
                    language="sql",
                )
        elif filter_pct < 1:
            with st.container(border=True):
                st.markdown("#### 💡 Performance Tip")
                st.markdown(
                    f"**Reason:** Very selective filter — only **{filter_pct:.2f}%** of rows were returned.  \n"
                    "The query is reading the whole table to find a tiny fraction of rows."
                )
                st.markdown(
                    "🔧 **Fix:** Z-ORDER or partition the table by the filter column so Databricks "
                    "can skip the irrelevant files entirely."
                )
                st.code(
                    "OPTIMIZE <catalog>.<schema>.<table>\n"
                    "ZORDER BY (<your_filter_column>);",
                    language="sql",
                )
        else:
            with st.container(border=True):
                st.markdown("#### ✅ No obvious performance issues")
                st.markdown(
                    "Scan size and filter ratio look reasonable for this query.  \n"
                    "If it still feels slow, check the operator breakdown below."
                )

        with st.expander("Full SQL"):
            st.code(str(row.statement_text), language="sql")

        # Operator-level profile from API (only available for queries < 7 days old)
        st.caption("**Operator breakdown** — only available for queries run within the last 7 days.")
        if st.button("Fetch operator profile", key="fetch_profile"):
            profile = _fetch_profile(str(row.statement_id))
            if "_error" in profile:
                if "404" in profile["_error"]:
                    st.info("Operator profile has expired (>7 days). The metrics above are always available.")
                else:
                    st.error(f"Could not fetch profile: {profile['_error']}")
                    with st.expander("Raw response"):
                        st.json(profile)
            else:
                nodes_df = _parse_profile(profile)
                if nodes_df.empty:
                    st.warning("Profile returned no operator nodes.")
                    with st.expander("Raw profile JSON"):
                        st.json(profile)
                else:
                    total_s = nodes_df["duration_s"].sum()
                    nodes_df["pct"] = (nodes_df["duration_s"] / total_s * 100).round(1)

                    fig_prof = px.bar(
                        nodes_df.head(20),
                        x="duration_s", y="operator",
                        orientation="h",
                        color="pct",
                        color_continuous_scale="Reds",
                        text=nodes_df.head(20)["duration_s"].apply(lambda v: f"{v:.3f}s"),
                        custom_data=["rows_out", "size_mb", "pct"],
                        labels={"duration_s": "Duration (s)", "operator": "", "pct": "% of total"},
                    )
                    fig_prof.update_traces(
                        textposition="outside",
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "Duration: %{x:.3f}s (%{customdata[2]:.1f}%)<br>"
                            "Rows out: %{customdata[0]:,}<br>"
                            "Size: %{customdata[1]:.2f} MB"
                            "<extra></extra>"
                        ),
                    )
                    fig_prof.update_layout(
                        height=max(300, len(nodes_df.head(20)) * 36),
                        margin=dict(l=0, r=80, t=10, b=0),
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig_prof, use_container_width=True)
                    st.dataframe(
                        nodes_df.rename(columns={
                            "operator": "Operator", "duration_s": "Duration (s)",
                            "rows_out": "Rows Out", "size_mb": "Size (MB)", "pct": "% Total",
                        }),
                        hide_index=True, use_container_width=True,
                        height=min(400, (len(nodes_df) + 1) * 38),
                    )

# ── Tab 2: DLT Pipeline Durations ─────────────────────────────────────────────
with perf_tab2:
    dlt_df = safe_run(f"""
    SELECT
      DATE(period_start_time)                                              AS run_date,
      COALESCE(pipeline_name, pipeline_id)                                AS pipeline,
      result_state,
      trigger_type,
      ROUND(
        (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0,
        1
      )                                                                    AS duration_min
    FROM {MONITORING_CATALOG}.system_billing.pipeline_update_timeline
    WHERE DATE(period_start_time) BETWEEN '{start_date}' AND '{end_date}'
      AND period_end_time IS NOT NULL
    ORDER BY run_date, pipeline
    """)

    if dlt_df.empty:
        st.info("No pipeline duration data found. Run **sync_system_billing_job** to populate the `pipeline_update_timeline` table, then refresh.")
    else:
        dlt_df["duration_min"] = dlt_df["duration_min"].astype(float)
        completed = dlt_df[dlt_df["result_state"] == "COMPLETED"]

        col_l, col_r = st.columns([3, 2])
        with col_l:
            st.subheader("Pipeline Duration Trend (minutes)")
            if not completed.empty:
                fig_dlt = px.line(
                    completed, x="run_date", y="duration_min", color="pipeline",
                    markers=True,
                    labels={"run_date": "", "duration_min": "Duration (min)", "pipeline": "Pipeline"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig_dlt.update_layout(
                    height=350, margin=dict(l=0, r=0, t=10, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                st.plotly_chart(fig_dlt, use_container_width=True)

        with col_r:
            st.subheader("Avg Duration by Pipeline (min)")
            avg_dlt = (
                completed.groupby("pipeline")
                .agg(
                    avg=("duration_min", "mean"),
                    runs=("duration_min", "count"),
                    p95=("duration_min", lambda x: x.quantile(0.95)),
                )
                .round(1)
                .reset_index()
                .sort_values("avg", ascending=False)
            )
            avg_dlt.columns = ["Pipeline", "Avg (min)", "Runs", "p95 (min)"]
            st.dataframe(avg_dlt, hide_index=True, use_container_width=True, height=350)

# ── Tab 3: Job Task Breakdown ──────────────────────────────────────────────────
with perf_tab3:
    task_df = safe_run(f"""
    SELECT
      DATE(period_start_time)                                                 AS run_date,
      job_id,
      task_key,
      result_state,
      ROUND(
        (UNIX_TIMESTAMP(period_end_time) - UNIX_TIMESTAMP(period_start_time)) / 60.0,
        1
      )                                                                       AS duration_min
    FROM {MONITORING_CATALOG}.system_billing.job_task_run_timeline
    WHERE DATE(period_start_time) BETWEEN '{start_date}' AND '{end_date}'
      AND period_end_time IS NOT NULL
    ORDER BY run_date, task_key
    """)

    if task_df.empty:
        st.info("No job task data found. Run **sync_system_billing_job** to populate the `job_task_run_timeline` table, then refresh.")
    else:
        task_df["duration_min"] = task_df["duration_min"].astype(float)

        st.subheader("Job Task Duration Heatmap (minutes)")
        st.caption("Colour intensity = duration. Spots the bottleneck task across runs at a glance.")

        pivot = (
            task_df[task_df["result_state"] == "SUCCESS"]
            .groupby(["task_key", "run_date"])["duration_min"]
            .mean()
            .reset_index()
            .pivot_table(index="task_key", columns="run_date", values="duration_min")
            .round(1)
        )
        if not pivot.empty:
            fig_heat = px.imshow(
                pivot,
                text_auto=".1f",
                color_continuous_scale="YlOrRd",
                labels={"color": "Duration (min)", "x": "Run Date", "y": "Task"},
                aspect="auto",
            )
            fig_heat.update_layout(
                height=max(300, len(pivot) * 45),
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="", yaxis_title="",
                coloraxis_colorbar=dict(title="min"),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

        st.subheader("Task Duration Over Time")
        fig_task = px.line(
            task_df[task_df["result_state"] == "SUCCESS"],
            x="run_date", y="duration_min", color="task_key",
            markers=True,
            labels={"run_date": "", "duration_min": "Duration (min)", "task_key": "Task"},
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_task.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_task, use_container_width=True)
