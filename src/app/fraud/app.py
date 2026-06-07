import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

GOLD_CATALOG = os.getenv("GOLD_CATALOG", "gold_dev")
FRAUD_TABLE  = f"{GOLD_CATALOG}.summary.summary_fraud_intelligence"

st.set_page_config(
    page_title="Fraud Intelligence",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Connection ──────────────────────────────────────────────────────────────────
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
    with _get_conn().cursor() as cur:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── Filter bootstrap ────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _get_filter_values():
    df = run(f"""
        SELECT DISTINCT month, incident_region, claim_type
        FROM {FRAUD_TABLE}
        ORDER BY month
    """)
    return (
        sorted(df["month"].dropna().unique().tolist()),
        sorted(df["incident_region"].dropna().unique().tolist()),
        sorted(df["claim_type"].dropna().unique().tolist()),
    )


def _where(month_range, regions, claim_types, storm_filter):
    clauses = [f"month BETWEEN '{month_range[0]}' AND '{month_range[1]}'"]
    if regions:
        r = ", ".join(f"'{v}'" for v in regions)
        clauses.append(f"incident_region IN ({r})")
    if claim_types:
        c = ", ".join(f"'{v}'" for v in claim_types)
        clauses.append(f"claim_type IN ({c})")
    if storm_filter == "Storm Freya only":
        clauses.append("is_storm_related = TRUE")
    elif storm_filter == "Non-storm only":
        clauses.append("is_storm_related = FALSE")
    return "WHERE " + " AND ".join(clauses)


# ── Sidebar ─────────────────────────────────────────────────────────────────────
months, all_regions, all_types = _get_filter_values()

with st.sidebar:
    st.title("⚙️ Filters")

    month_range = st.select_slider(
        "Month Range",
        options=months,
        value=(months[0], months[-1]),
    )
    sel_regions = st.multiselect("Regions",     all_regions, placeholder="All regions")
    sel_types   = st.multiselect("Claim Types", all_types,   placeholder="All types")
    storm_filter = st.radio(
        "Storm Filter",
        ["All claims", "Storm Freya only", "Non-storm only"],
        index=0,
    )

    st.divider()
    if st.button("🔄 Refresh", use_container_width=True):
        run.clear()
        _get_filter_values.clear()
        st.rerun()

    st.caption(f"**Source:** `{FRAUD_TABLE}`")

WHERE = _where(month_range, sel_regions, sel_types, storm_filter)

# ── Header ───────────────────────────────────────────────────────────────────────
st.title("🚨 Fraud Intelligence Dashboard")
st.caption(
    f"**{month_range[0]} → {month_range[1]}**"
    + (f" · {', '.join(sel_regions)}" if sel_regions else " · All regions")
    + (f" · {', '.join(sel_types)}"   if sel_types   else " · All claim types")
    + f" · {storm_filter}"
)

# ── KPIs ─────────────────────────────────────────────────────────────────────────
kpi = run(f"""
    SELECT
      SUM(total_claims)                                                             AS total_claims,
      SUM(flagged_claims)                                                           AS flagged_claims,
      ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2)          AS fraud_rate_pct,
      ROUND(SUM(flagged_claims_gbp), 0)                                            AS fraud_amount_gbp,
      ROUND(SUM(flagged_claims_gbp) / NULLIF(SUM(total_claims_gbp), 0) * 100, 2) AS fraud_amount_ratio_pct,
      ROUND(AVG(avg_fraud_score), 3)                                               AS avg_fraud_score
    FROM {FRAUD_TABLE}
    {WHERE}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Claims",         f"{int(kpi.total_claims):,}")
c2.metric("Flagged Claims",       f"{int(kpi.flagged_claims):,}",
          help="Claims with fraud_risk_score ≥ 0.6")
c3.metric("Fraud Rate",           f"{float(kpi.fraud_rate_pct):.1f}%",
          delta=f"Amount ratio {float(kpi.fraud_amount_ratio_pct):.1f}%",
          delta_color="off",
          help="Flagged claims ÷ total claims")
c4.metric("Fraud Amount at Risk", f"£{float(kpi.fraud_amount_gbp):,.0f}",
          help="Total GBP value of flagged claims")

st.divider()

# ── Row 2: Fraud Rate by Region | Storm vs Baseline ────────────────────────────
col_l, col_r = st.columns([3, 2])

with col_l:
    st.subheader("Fraud Rate by Region")
    region_df = run(f"""
        SELECT
          incident_region,
          SUM(total_claims)   AS total_claims,
          SUM(flagged_claims) AS flagged_claims,
          ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2) AS fraud_rate_pct,
          ROUND(SUM(flagged_claims_gbp), 0)                                   AS fraud_amount_gbp
        FROM {FRAUD_TABLE}
        {WHERE}
        GROUP BY incident_region
        ORDER BY fraud_rate_pct DESC
    """)
    region_df["fraud_rate_pct"]   = region_df["fraud_rate_pct"].astype(float)
    region_df["fraud_amount_gbp"] = region_df["fraud_amount_gbp"].astype(float)

    fig_region = px.bar(
        region_df, x="fraud_rate_pct", y="incident_region", orientation="h",
        color="fraud_rate_pct", color_continuous_scale="Reds",
        text=region_df["fraud_rate_pct"].apply(lambda v: f"{v:.1f}%"),
        labels={"fraud_rate_pct": "Fraud Rate (%)", "incident_region": ""},
        custom_data=["total_claims", "flagged_claims", "fraud_amount_gbp"],
    )
    fig_region.update_traces(
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Fraud Rate: %{x:.1f}%<br>"
            "Flagged: %{customdata[1]:,} of %{customdata[0]:,}<br>"
            "Amount at Risk: £%{customdata[2]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig_region.update_layout(
        height=400, coloraxis_showscale=False,
        margin=dict(l=0, r=60, t=10, b=0),
        xaxis_title="Fraud Rate (%)",
    )
    st.plotly_chart(fig_region, use_container_width=True)

with col_r:
    st.subheader("Storm Freya vs Baseline")
    storm_df = run(f"""
        SELECT
          CASE WHEN is_storm_related THEN 'Storm Freya' ELSE 'Baseline' END AS period_type,
          ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2)            AS fraud_rate_pct,
          ROUND(SUM(flagged_claims_gbp) / NULLIF(SUM(total_claims_gbp), 0) * 100, 2)   AS fraud_amount_ratio_pct,
          SUM(total_claims)   AS total_claims,
          SUM(flagged_claims) AS flagged_claims
        FROM {FRAUD_TABLE}
        {WHERE}
        GROUP BY is_storm_related
        ORDER BY is_storm_related DESC
    """)
    storm_df["fraud_rate_pct"]         = storm_df["fraud_rate_pct"].astype(float)
    storm_df["fraud_amount_ratio_pct"] = storm_df["fraud_amount_ratio_pct"].astype(float)

    fig_storm = go.Figure()
    palette = {"Storm Freya": ("#D32F2F", "#FF7043"), "Baseline": ("#1565C0", "#42A5F5")}
    for _, row in storm_df.iterrows():
        ptype = row["period_type"]
        col_a, col_b = palette.get(ptype, ("#888", "#aaa"))
        fig_storm.add_trace(go.Bar(
            name=f"{ptype} — Fraud Rate %",
            x=[ptype], y=[row["fraud_rate_pct"]],
            marker_color=col_a,
            text=[f"{row['fraud_rate_pct']:.1f}%"], textposition="outside",
        ))
        fig_storm.add_trace(go.Bar(
            name=f"{ptype} — Amount Ratio %",
            x=[ptype], y=[row["fraud_amount_ratio_pct"]],
            marker_color=col_b,
            text=[f"{row['fraud_amount_ratio_pct']:.1f}%"], textposition="outside",
        ))
    fig_storm.update_layout(
        height=400, barmode="group",
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis_title="%",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        showlegend=True,
    )
    st.plotly_chart(fig_storm, use_container_width=True)

# ── Row 3: Trend Over Time ───────────────────────────────────────────────────────
st.subheader("Fraud Rate Trend by Month")
trend_df = run(f"""
    SELECT
      month,
      claim_type,
      ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2) AS fraud_rate_pct,
      SUM(flagged_claims) AS flagged_claims,
      SUM(total_claims)   AS total_claims
    FROM {FRAUD_TABLE}
    {WHERE}
    GROUP BY month, claim_type
    ORDER BY month, claim_type
""")
trend_df["fraud_rate_pct"] = trend_df["fraud_rate_pct"].astype(float)

fig_trend = px.line(
    trend_df, x="month", y="fraud_rate_pct", color="claim_type",
    markers=True,
    labels={"month": "", "fraud_rate_pct": "Fraud Rate (%)", "claim_type": "Claim Type"},
    color_discrete_sequence=px.colors.qualitative.Set1,
    custom_data=["flagged_claims", "total_claims"],
)
fig_trend.update_traces(
    hovertemplate=(
        "<b>%{x} · %{fullData.name}</b><br>"
        "Fraud Rate: %{y:.1f}%<br>"
        "Flagged: %{customdata[0]:,} of %{customdata[1]:,}"
        "<extra></extra>"
    )
)
fig_trend.update_layout(
    height=320, margin=dict(l=0, r=0, t=10, b=0),
    yaxis_title="Fraud Rate (%)", hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ── Row 4: Fraud by Claim Type | Region Scatter ─────────────────────────────────
col_l2, col_r2 = st.columns([3, 2])

with col_l2:
    st.subheader("Fraud Rate by Claim Type")
    type_df = run(f"""
        SELECT
          claim_type,
          SUM(total_claims)   AS total_claims,
          SUM(flagged_claims) AS flagged_claims,
          ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2) AS fraud_rate_pct,
          ROUND(AVG(avg_fraud_score), 3)                                       AS avg_fraud_score,
          ROUND(SUM(flagged_claims_gbp), 0)                                    AS fraud_amount_gbp
        FROM {FRAUD_TABLE}
        {WHERE}
        GROUP BY claim_type
        ORDER BY fraud_rate_pct DESC
    """)
    for col in ["fraud_rate_pct", "avg_fraud_score", "fraud_amount_gbp"]:
        type_df[col] = type_df[col].astype(float)

    fig_type = px.bar(
        type_df, x="claim_type", y="fraud_rate_pct",
        color="fraud_rate_pct", color_continuous_scale="Reds",
        text=type_df["fraud_rate_pct"].apply(lambda v: f"{v:.1f}%"),
        labels={"claim_type": "", "fraud_rate_pct": "Fraud Rate (%)"},
        custom_data=["total_claims", "flagged_claims", "fraud_amount_gbp", "avg_fraud_score"],
    )
    fig_type.update_traces(
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Fraud Rate: %{y:.1f}%<br>"
            "Flagged: %{customdata[1]:,} of %{customdata[0]:,}<br>"
            "Amount at Risk: £%{customdata[2]:,.0f}<br>"
            "Avg Fraud Score: %{customdata[3]:.3f}"
            "<extra></extra>"
        ),
    )
    fig_type.update_layout(
        height=340, coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=30, b=0),
    )
    st.plotly_chart(fig_type, use_container_width=True)

with col_r2:
    st.subheader("Fraud Rate vs Amount Ratio by Region")
    scatter_df = run(f"""
        SELECT
          incident_region,
          ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2)           AS fraud_rate_pct,
          ROUND(SUM(flagged_claims_gbp) / NULLIF(SUM(total_claims_gbp), 0) * 100, 2)  AS fraud_amount_ratio_pct,
          SUM(total_claims)                                                              AS total_claims,
          ROUND(SUM(flagged_claims_gbp), 0)                                             AS fraud_amount_gbp
        FROM {FRAUD_TABLE}
        {WHERE}
        GROUP BY incident_region
    """)
    for col in ["fraud_rate_pct", "fraud_amount_ratio_pct", "fraud_amount_gbp"]:
        scatter_df[col] = scatter_df[col].astype(float)

    fig_scatter = px.scatter(
        scatter_df, x="fraud_rate_pct", y="fraud_amount_ratio_pct",
        text="incident_region", size="total_claims",
        color="fraud_rate_pct", color_continuous_scale="Reds",
        labels={
            "fraud_rate_pct":         "Fraud Rate (%)",
            "fraud_amount_ratio_pct": "Fraud Amount Ratio (%)",
        },
        custom_data=["fraud_amount_gbp", "total_claims"],
    )
    fig_scatter.update_traces(
        textposition="top center",
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Fraud Rate: %{x:.1f}%<br>"
            "Amount Ratio: %{y:.1f}%<br>"
            "Amount at Risk: £%{customdata[0]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig_scatter.update_layout(
        height=340, coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=30, b=0),
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ── Row 5: Heatmap — Region × Claim Type ───────────────────────────────────────
st.subheader("Fraud Rate Heatmap — Region × Claim Type (%)")
heatmap_df = run(f"""
    SELECT
      incident_region,
      claim_type,
      ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2) AS fraud_rate_pct
    FROM {FRAUD_TABLE}
    {WHERE}
    GROUP BY incident_region, claim_type
""")
heatmap_df["fraud_rate_pct"] = heatmap_df["fraud_rate_pct"].astype(float)

if not heatmap_df.empty:
    pivot = (
        heatmap_df
        .pivot_table(index="incident_region", columns="claim_type",
                     values="fraud_rate_pct", aggfunc="mean")
        .round(1)
    )
    fig_heat = px.imshow(
        pivot, text_auto=".1f",
        color_continuous_scale="Reds",
        labels={"color": "Fraud Rate (%)", "x": "Claim Type", "y": "Region"},
        aspect="auto",
    )
    fig_heat.update_layout(
        height=max(320, len(pivot) * 38),
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_colorbar=dict(title="Fraud Rate (%)"),
        xaxis_title="", yaxis_title="",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

# ── Row 6: Detail Table ──────────────────────────────────────────────────────────
st.divider()
st.subheader("Detailed Breakdown")
detail_df = run(f"""
    SELECT
      month,
      incident_region                                                              AS region,
      claim_type,
      CASE WHEN is_storm_related THEN 'Yes' ELSE 'No' END                         AS storm,
      SUM(total_claims)                                                            AS total_claims,
      SUM(flagged_claims)                                                          AS flagged_claims,
      ROUND(SUM(flagged_claims) / NULLIF(SUM(total_claims), 0) * 100, 2)         AS fraud_rate_pct,
      ROUND(AVG(avg_fraud_score), 3)                                              AS avg_fraud_score,
      ROUND(SUM(flagged_claims_gbp), 0)                                           AS fraud_amount_gbp,
      ROUND(SUM(flagged_claims_gbp) / NULLIF(SUM(total_claims_gbp), 0) * 100, 2) AS fraud_amount_ratio_pct
    FROM {FRAUD_TABLE}
    {WHERE}
    GROUP BY month, incident_region, claim_type, is_storm_related
    ORDER BY fraud_rate_pct DESC, month
""")
for col in ["fraud_rate_pct", "avg_fraud_score", "fraud_amount_gbp", "fraud_amount_ratio_pct"]:
    detail_df[col] = detail_df[col].astype(float)

st.dataframe(
    detail_df.rename(columns={
        "month": "Month", "region": "Region", "claim_type": "Claim Type",
        "storm": "Storm", "total_claims": "Total", "flagged_claims": "Flagged",
        "fraud_rate_pct": "Fraud Rate %", "avg_fraud_score": "Avg Score",
        "fraud_amount_gbp": "Fraud £", "fraud_amount_ratio_pct": "Amount Ratio %",
    }),
    hide_index=True,
    use_container_width=True,
    height=400,
)
