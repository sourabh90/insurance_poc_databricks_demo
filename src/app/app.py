import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

# ── Page config — must be the first Streamlit call ────────────────────────────
st.set_page_config(
    page_title="Databricks Billing Usage",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── SQL building blocks ────────────────────────────────────────────────────────
_PRICE_JOIN = """
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)"""

_FX_JOIN = """
  LEFT JOIN (
    SELECT rate_date, usd_to_gbp,
      COALESCE(LEAD(rate_date) OVER (ORDER BY rate_date), DATE_ADD(rate_date, 365)) AS next_rate_date
    FROM bronze_dev.raw_reference.fx_rates_usd_gbp
  ) fx ON u.usage_date >= fx.rate_date AND u.usage_date < fx.next_rate_date"""

_PIPELINE_JOIN = """
  LEFT JOIN (
    SELECT DISTINCT pipeline_id, FIRST(name) AS name
    FROM system.lakeflow.pipelines GROUP BY pipeline_id
  ) pl ON u.usage_metadata.dlt_pipeline_id = pl.pipeline_id"""

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
FROM system.billing.usage u
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
FROM system.billing.usage u
{_PRICE_JOIN}
{_FX_JOIN}
WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
GROUP BY 1, 2
ORDER BY 1
""")
time_df["cost_gbp"] = time_df["cost_gbp"].astype(float)
total_time = time_df.groupby("period", as_index=False)["cost_gbp"].sum()

if show_forecast:
    fc_horizon = (today + timedelta(days=30)).strftime("%Y-%m-%d")
    fc_df = run(f"""
    SELECT ds AS period, GREATEST(y_forecast, 0) AS cost_gbp
    FROM ai_forecast(
      TABLE(
        SELECT u.usage_date AS ds,
          SUM(CASE WHEN u.usage_unit='DBU' THEN {_COST} ELSE 0 END) AS y
        FROM system.billing.usage u
        {_PRICE_JOIN}
        {_FX_JOIN}
        WHERE u.record_type = 'ORIGINAL' AND {_date_filter}
        GROUP BY u.usage_date ORDER BY u.usage_date
      ),
      horizon => '{fc_horizon}',
      time_col => 'ds',
      value_col => 'y'
    )
    """)
    fc_df["cost_gbp"] = fc_df["cost_gbp"].astype(float)

    fig_time = go.Figure()
    fig_time.add_trace(go.Scatter(
        x=total_time["period"], y=total_time["cost_gbp"],
        name="Actual", fill="tozeroy",
        line=dict(color="#1B76C2", width=2),
    ))
    fig_time.add_trace(go.Scatter(
        x=fc_df["period"], y=fc_df["cost_gbp"],
        name="Forecast", line=dict(color="#FF3621", width=2, dash="dash"),
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
    FROM system.billing.usage u
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
      FROM system.billing.usage u
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
FROM system.billing.usage u
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
