"""
Urban Stress Intelligence Platform — Streamlit Dashboard
=========================================================
Data source: RDS PostgreSQL datamart via psycopg2.
All fetch functions are cached with ttl=3600 (1 hour).
Set DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD as env vars.
"""

import os
import psycopg2
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, date, timedelta

# set_page_config MUST be the first Streamlit call
st.set_page_config(
    page_title="Urban Stress Intelligence Platform",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

CITY = "Boston, MA"

DOCS_BASE_URL = "https://github.com/your-org/urban-stress-platform/blob/main/docs/metrics.md"

# ── Static metric metadata (tooltips, units, anchors) ─────────────────────────
METRIC_META = {
    "open_backlog": {
        "label": "311 open backlog", "unit": "requests",
        "source_field": "COUNT(service_request_id) WHERE status='open'",
        "tooltip": (
            "Total Open311 service requests with status='open' as of today. "
            "Delta shows % change vs the 30-day rolling average. "
            "A rising backlog signals demand is outpacing resolution capacity."
        ),
        "docs_anchor": "metric-open-backlog",
    },
    "mean_close_hrs": {
        "label": "Mean close time", "unit": "hrs",
        "source_field": "AVG(updated_datetime - requested_datetime) WHERE status='closed'",
        "tooltip": (
            "Average hours between requested_datetime and updated_datetime "
            "for requests closed today. Rising close time indicates resource strain."
        ),
        "docs_anchor": "metric-mean-close-time",
    },
    "top_category": {
        "label": "Top complaint category", "unit": "",
        "source_field": "MODE(service_name) — today's highest volume category",
        "tooltip": (
            "The service_name with the highest request count today. "
            "Useful for directing 311 queues and pre-staffing the relevant agency."
        ),
        "docs_anchor": "metric-top-category",
    },
    "tmax": {
        "label": "TMAX today", "unit": "°C",
        "source_field": "weather_daily.tmax · NWS KBOS station",
        "tooltip": (
            "Maximum daily temperature from NWS KBOS (Logan Airport). "
            "Used to compute heat_score when heat index is unavailable (winter)."
        ),
        "docs_anchor": "metric-tmax",
    },
    "heat_streak_days": {
        "label": "Heat day streak", "unit": "consecutive days ≥ 40°C heat index",
        "source_field": "COUNT consecutive days WHERE heat_index_max > 40",
        "tooltip": (
            "Consecutive days where NWS heat index exceeded 40°C. "
            "Streaks amplify the heat_score in the composite."
        ),
        "docs_anchor": "metric-heat-streak",
    },
    "media_tone_7d": {
        "label": "Media tone (7d avg)", "unit": "tone score",
        "source_field": "gdelt_daily.avg_tone_7d · 7-day rolling average",
        "tooltip": (
            "7-day rolling average of GDELT AvgTone for Boston news coverage. "
            "Scale: −100 (extremely negative) to +100 (extremely positive). "
            "Situational context only — reflects reported events, not future ones."
        ),
        "docs_anchor": "metric-media-tone",
    },
    "protest_mentions": {
        "label": "Protest mentions", "unit": "events today",
        "source_field": "gdelt_daily.protest_event_count · EventRootCode='14'",
        "tooltip": (
            "GDELT events with EventRootCode='14' (PROTEST) mentioning Boston today. "
            "Reflects media-reported protest activity already in the public record."
        ),
        "docs_anchor": "metric-protest-mentions",
    },
    "precip_72hr_mm": {
        "label": "Precip last 72 hrs", "unit": "mm",
        "source_field": "SUM(weather_daily.precip_6hr_max) last 3 days",
        "tooltip": (
            "Total precipitation over the previous 72 hours from NWS KBOS. "
            "High values (>25mm) indicate storm pressure on infrastructure."
        ),
        "docs_anchor": "metric-precip",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )
    # autocommit = True: each SELECT is its own implicit transaction.
    # Prevents one failed query from poisoning the shared cached connection.
    conn.autocommit = True
    return conn

# ─────────────────────────────────────────────────────────────────────────────
# FETCH FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def fetch_composite(city="Boston"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT composite, composite_baseline_30d,
                   composite - composite_baseline_30d AS delta,
                   readiness_mode, score_date
            FROM signal_scores
            WHERE city = %s
            ORDER BY score_date DESC LIMIT 1
        """, [city])
        row = cur.fetchone()
    if not row:
        return 0.0, 0.0, 0.0, "Normal", str(date.today())
    return float(row[0]), float(row[1]), float(row[2]), row[3], str(row[4])

@st.cache_data(ttl=3600)
def fetch_last_updated():
    conn = get_connection()

    # Level 1 — pipeline_runs table (exact Step Functions completion timestamp)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(run_at) FROM pipeline_runs WHERE status = 'success'"
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            last_run = row[0]
            return (
                last_run.strftime("%Y-%m-%d %H:%M UTC"),
                (last_run - timedelta(days=1)).strftime("%Y-%m-%d"),
            )
    except Exception:
        pass

    # Level 2 — signal_scores latest score_date (always present after any ETL run)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(score_date) FROM signal_scores WHERE city = 'Boston'"
            )
            row = cur.fetchone()
        if row and row[0] is not None:
            score_date = row[0]
            # score_date is the obs_date the ETL ran for (yesterday's data)
            return (
                f"{str(score_date)} (last scored date)",
                str(score_date),
            )
    except Exception:
        pass

    # Level 3 — nothing in DB yet
    return "N/A", str(date.today() - timedelta(days=1))

@st.cache_data(ttl=3600)
def fetch_verdict(city="Boston"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT verdict_text, generated_at, model_id
            FROM verdicts
            WHERE city = %s
            ORDER BY verdict_date DESC LIMIT 1
        """, [city])
        row = cur.fetchone()
    if not row:
        return "No verdict available yet.", "N/A", "rule-based-v1"
    generated = row[1].strftime("%Y-%m-%d %H:%M UTC") if row[1] else "N/A"
    return row[0], generated, row[2]

@st.cache_data(ttl=3600)
def fetch_metrics(city="Boston"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT metric_name, value_num, value_text, delta_pct, baseline
            FROM signal_metrics
            WHERE city = %s
              AND metric_date = (
                SELECT MAX(metric_date) FROM signal_metrics WHERE city = %s
              )
        """, [city, city])
        rows = cur.fetchall()
    return {r[0]: {"value_num": r[1], "value_text": r[2],
                   "delta_pct": r[3], "baseline": r[4]} for r in rows}

@st.cache_data(ttl=3600)
def fetch_history_month(city="Boston"):
    conn = get_connection()
    df_scores = pd.read_sql("""
        SELECT score_date::text AS date,
               composite, composite_baseline_30d AS comp_base
        FROM signal_scores
        WHERE city = %s
        ORDER BY score_date DESC LIMIT 30
    """, conn, params=[city])
    df_weather = pd.read_sql("""
        SELECT obs_date::text AS date, tmax
        FROM weather_daily
        ORDER BY obs_date DESC LIMIT 30
    """, conn)
    df_gdelt = pd.read_sql("""
        SELECT obs_date::text AS date, avg_tone_7d AS tone
        FROM gdelt_daily
        WHERE city = %s
        ORDER BY obs_date DESC LIMIT 30
    """, conn, params=[city])
    df_311 = pd.read_sql("""
        SELECT DATE(requested_datetime)::text AS date,
               COUNT(*) AS vol_311
        FROM requests_311
        GROUP BY DATE(requested_datetime)
        ORDER BY DATE(requested_datetime) DESC LIMIT 30
    """, conn)
    df = df_scores.merge(df_weather, on="date", how="left") \
                  .merge(df_gdelt,   on="date", how="left") \
                  .merge(df_311,     on="date", how="left")
    df = df.sort_values("date")
    df["base_311"] = df["vol_311"].mean()
    df = df.fillna(0)
    return df

@st.cache_data(ttl=3600)
def fetch_history_year(city="Boston"):
    conn = get_connection()
    df_scores = pd.read_sql("""
        SELECT TO_CHAR(score_date, 'Mon') AS month,
               EXTRACT(MONTH FROM score_date) AS month_num,
               AVG(composite) AS composite,
               AVG(composite_baseline_30d) AS comp_base
        FROM signal_scores
        WHERE city = %s
        GROUP BY TO_CHAR(score_date, 'Mon'), EXTRACT(MONTH FROM score_date)
        ORDER BY month_num
    """, conn, params=[city])
    df_weather = pd.read_sql("""
        SELECT TO_CHAR(obs_date, 'Mon') AS month,
               EXTRACT(MONTH FROM obs_date) AS month_num,
               AVG(tmax) AS tmax
        FROM weather_daily
        GROUP BY TO_CHAR(obs_date, 'Mon'), EXTRACT(MONTH FROM obs_date)
        ORDER BY month_num
    """, conn)
    df_gdelt = pd.read_sql("""
        SELECT TO_CHAR(obs_date, 'Mon') AS month,
               EXTRACT(MONTH FROM obs_date) AS month_num,
               AVG(avg_tone_7d) AS tone
        FROM gdelt_daily
        WHERE city = %s
        GROUP BY TO_CHAR(obs_date, 'Mon'), EXTRACT(MONTH FROM obs_date)
        ORDER BY month_num
    """, conn, params=[city])
    df_311 = pd.read_sql("""
        SELECT TO_CHAR(DATE(requested_datetime), 'Mon') AS month,
               EXTRACT(MONTH FROM requested_datetime) AS month_num,
               COUNT(*) AS vol_311
        FROM requests_311
        GROUP BY TO_CHAR(DATE(requested_datetime), 'Mon'),
                 EXTRACT(MONTH FROM requested_datetime)
        ORDER BY month_num
    """, conn)
    df = df_scores.merge(df_weather, on=["month","month_num"], how="left") \
                  .merge(df_gdelt,   on=["month","month_num"], how="left") \
                  .merge(df_311,     on=["month","month_num"], how="left")
    df["base_311"] = df["vol_311"].mean()
    df = df.fillna(0)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────

COMPOSITE_SCORE, COMPOSITE_BASELINE, COMPOSITE_DELTA, READINESS_MODE, DATA_AS_OF = fetch_composite()
LAST_UPDATED, _  = fetch_last_updated()
LLM_VERDICT, LLM_GENERATED_AT, MODEL_ID = fetch_verdict()
DB_METRICS       = fetch_metrics()
MONTH_DATA       = fetch_history_month()
YEAR_DATA        = fetch_history_year()

# ── Build METRICS dict from DB + static metadata ──────────────────────────────
METRICS = {}
for key, meta in METRIC_META.items():
    db = DB_METRICS.get(key, {})
    value = db.get("value_text") or db.get("value_num") or "N/A"
    METRICS[meta["label"]] = {
        "value":        value,
        "unit":         meta["unit"],
        "delta_pct":    float(db["delta_pct"]) if db.get("delta_pct") is not None else None,
        "baseline":     float(db["baseline"])  if db.get("baseline")  is not None else None,
        "source_field": meta["source_field"],
        "tooltip":      meta["tooltip"],
        "docs_anchor":  meta["docs_anchor"],
    }

st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
  .metric-card {
    background: var(--background-color);
    border: 0.5px solid rgba(120,120,120,0.2);
    border-radius: 10px;
    padding: 14px 16px 12px;
    position: relative;
    min-height: 100px;
  }
  .metric-label  { font-size: 11px; color: #888; text-transform: uppercase;
                   letter-spacing: .06em; margin-bottom: 4px; }
  .metric-value  { font-size: 26px; font-weight: 600; line-height: 1.1; }
  .metric-unit   { font-size: 12px; color: #888; margin-top: 2px; }
  .metric-delta-up   { color: #c0392b; font-size: 12px; font-weight: 500; }
  .metric-delta-down { color: #1a7a4a; font-size: 12px; font-weight: 500; }
  .metric-delta-neu  { color: #888;    font-size: 12px; }
  .info-icon {
    position: absolute; top: 10px; right: 10px;
    width: 18px; height: 18px; border-radius: 50%;
    border: 1px solid #aaa; color: #aaa;
    font-size: 11px; font-weight: 600;
    display: flex; align-items: center; justify-content: center;
    cursor: default; line-height: 1;
    z-index: 10;
  }
  /* Styled hover tooltip — replaces browser title= attribute */
  .tooltip-text {
    visibility: hidden;
    opacity: 0;
    background-color: #1e1e2e;
    color: #e8e8f0;
    text-align: left;
    border-radius: 8px;
    padding: 10px 13px;
    position: absolute;
    z-index: 9999;
    bottom: 130%;
    right: 0;
    width: 250px;
    font-size: 11.5px;
    font-weight: 400;
    line-height: 1.65;
    box-shadow: 0 6px 20px rgba(0,0,0,0.25);
    pointer-events: none;
    transition: opacity 0.15s ease;
    white-space: normal;
    border: 0.5px solid rgba(200,200,255,0.15);
  }
  .tooltip-text::after {
    content: "";
    position: absolute;
    top: 100%; right: 6px;
    border-width: 5px;
    border-style: solid;
    border-color: #1e1e2e transparent transparent transparent;
  }
  .info-icon:hover .tooltip-text {
    visibility: visible;
    opacity: 1;
  }
  .readiness-normal   { background:#EAF3DE; color:#27500A;
                        border:0.5px solid #639922; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }
  .readiness-elevated { background:#FAEEDA; color:#412402;
                        border:0.5px solid #BA7517; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }
  .readiness-high     { background:#FCEBEB; color:#501313;
                        border:0.5px solid #E24B4A; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }
  .human-bar {
    background: #EEEDFE; border: 0.5px solid #AFA9EC;
    border-radius: 8px; padding: 10px 14px;
    font-size: 12px; color: #3C3489; margin-top: 8px;
  }
  .verdict-box {
    background: rgba(120,120,120,0.06);
    border-left: 3px solid #7F77DD;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px; margin-top: 10px;
    font-size: 13px; line-height: 1.7;
  }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def delta_html(delta_pct):
    if delta_pct is None:
        return ""
    sign = "+" if delta_pct >= 0 else ""
    cls  = "metric-delta-up" if delta_pct > 0 else ("metric-delta-down" if delta_pct < 0 else "metric-delta-neu")
    return f'<span class="{cls}">{sign}{delta_pct:.1f}% vs 30d avg</span>'


def metric_card(label, value, unit, delta_pct, tooltip, docs_anchor):
    docs_url = f"{DOCS_BASE_URL}#{docs_anchor}"
    d_html   = delta_html(delta_pct)
    val_str  = str(value) if not isinstance(value, float) else f"{value:.1f}"
    st.markdown(f"""
    <div class="metric-card">
      <div class="info-icon">i<span class="tooltip-text">{tooltip}</span></div>
      <div class="metric-label">{label}</div>
      <div class="metric-value">{val_str}</div>
      <div class="metric-unit">{unit}</div>
      {d_html}
    </div>
    <div style="font-size:10px;margin:4px 0 12px;padding-left:2px;">
      <a href="{docs_url}" target="_blank" style="color:#7F77DD;text-decoration:none;">
        View metric docs ↗
      </a>
    </div>
    """, unsafe_allow_html=True)


def readiness_badge(mode):
    cls = f"readiness-{mode.lower()}"
    return f'<span class="{cls}">{mode} context</span>'


def composite_gauge(score, baseline):
    colour = "#27500A" if score < 40 else ("#854F0B" if score < 65 else "#A32D2D")
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        delta={"reference": baseline, "valueformat": ".0f",
               "increasing": {"color": "#c0392b"}, "decreasing": {"color": "#1a7a4a"}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#888"},
            "bar":  {"color": colour, "thickness": 0.25},
            "bgcolor": "rgba(0,0,0,0)",
            "steps": [
                {"range": [0,  40], "color": "#EAF3DE"},
                {"range": [40, 65], "color": "#FAEEDA"},
                {"range": [65,100], "color": "#FCEBEB"},
            ],
            "threshold": {
                "line": {"color": "#888", "width": 2},
                "thickness": 0.75, "value": baseline,
            },
        },
        number={"font": {"size": 40, "color": colour}},
        title={"text": "Composite stress index", "font": {"size": 13, "color": "#888"}},
    ))
    fig.update_layout(
        height=220, margin=dict(t=30, b=10, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font={"family": "system-ui,sans-serif"},
    )
    return fig


def make_311_chart(df, x_col, is_year):
    fig = go.Figure()
    fig.add_bar(x=df[x_col], y=df["vol_311"], name="Requests",
                marker_color="rgba(29,158,117,0.4)", marker_line_color="#1D9E75",
                marker_line_width=0.5, hovertemplate="%{y} requests<extra></extra>")
    fig.add_scatter(x=df[x_col], y=df["base_311"], name="Baseline",
                    line=dict(color="#9FE1CB", width=1.5, dash="dash"),
                    mode="lines", hovertemplate="%{y} baseline<extra></extra>")
    fig.update_layout(
        title=dict(text="311 request volume", font=dict(size=12), x=0),
        height=200, margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=10)),
        yaxis=dict(title="req/day" if not is_year else "req/month",
                   gridcolor="#E8E6DF", tickfont=dict(size=10)),
        xaxis=dict(gridcolor="#E8E6DF", tickfont=dict(size=10)),
        hovermode="x unified",
    )
    return fig


def make_env_chart(df, x_col):
    fig = go.Figure()
    fig.add_scatter(x=df[x_col], y=df["tmax"], name="TMAX °C",
                    line=dict(color="#BA7517", width=2),
                    mode="lines+markers", marker=dict(size=4, symbol="circle"),
                    yaxis="y1",
                    hovertemplate="TMAX: %{y}°C<extra></extra>")
    fig.add_scatter(x=df[x_col], y=df["tone"], name="Media tone",
                    line=dict(color="#7F77DD", width=2, dash="dash"),
                    mode="lines+markers", marker=dict(size=4, symbol="triangle-up"),
                    yaxis="y2",
                    hovertemplate="Tone: %{y:.1f}<extra></extra>")
    fig.update_layout(
        title=dict(text="Environmental & media context", font=dict(size=12), x=0),
        height=200, margin=dict(t=30, b=30, l=50, r=50),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=10)),
        yaxis=dict(title=dict(text="°C", font=dict(color="#BA7517")),
                   gridcolor="#E8E6DF", tickfont=dict(size=10, color="#BA7517")),
        yaxis2=dict(title=dict(text="tone (0=neutral)", font=dict(color="#7F77DD")),
                    overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)", tickfont=dict(size=10),
                    range=[min(df["tone"])-0.5, 0.2]),
        xaxis=dict(gridcolor="#E8E6DF", tickfont=dict(size=10)),
        hovermode="x unified",
    )
    return fig


def make_composite_chart(df, x_col):
    fig = go.Figure()
    fig.add_scatter(x=df[x_col], y=df["comp_base"], name="Baseline",
                    line=dict(color="#B4B2A9", width=1.5, dash="dash"),
                    mode="lines",
                    hovertemplate="Baseline: %{y}<extra></extra>")
    fig.add_scatter(x=df[x_col], y=df["composite"], name="Composite score",
                    line=dict(color="#26215C", width=2.5),
                    mode="lines+markers",
                    marker=dict(size=4, symbol="diamond"),
                    fill="tonexty", fillcolor="rgba(83,74,183,0.06)",
                    hovertemplate="Score: %{y}<extra></extra>")
    fig.update_layout(
        title=dict(text="Composite stress index (0–100)", font=dict(size=12), x=0),
        height=200, margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=10)),
        yaxis=dict(title="score", range=[0, 100], gridcolor="#E8E6DF",
                   tickfont=dict(size=10)),
        xaxis=dict(gridcolor="#E8E6DF", tickfont=dict(size=10)),
        hovermode="x unified",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. TITLE ─────────────────────────────────────────────────────────────────
col_title, col_badge, col_refresh = st.columns([3, 1, 0.4])
with col_title:
    st.markdown(f"## 🏙️ Urban Stress Intelligence — {CITY}")
with col_badge:
    st.markdown(
        f'<div style="text-align:right;padding-top:14px">'
        f'{readiness_badge(READINESS_MODE)}</div>',
        unsafe_allow_html=True,
    )
with col_refresh:
    if st.button("↻ Refresh", help="Clear cached data and reload from RDS"):
        st.cache_data.clear()
        st.rerun()

# ── 2. UPDATED STATUS ────────────────────────────────────────────────────────
st.markdown(
    f'<p style="font-size:11px;color:#888;margin:-6px 0 18px">'
    f'Pipeline updated: <strong>{LAST_UPDATED}</strong> &nbsp;·&nbsp; '
    f'Data as of: <strong>{DATA_AS_OF}</strong> &nbsp;·&nbsp; '
    f'Human review required before operational action</p>',
    unsafe_allow_html=True,
)
st.divider()

# ── 3. COMPOSITE STRESS INDEX + LLM VERDICT ──────────────────────────────────
st.markdown("### Composite stress index & contextual analysis")
col_gauge, col_verdict = st.columns([1, 2])

with col_gauge:
    st.plotly_chart(composite_gauge(COMPOSITE_SCORE, COMPOSITE_BASELINE),
                    use_container_width=True, config={"displayModeBar": False})
    st.markdown(
        f'<p style="font-size:11px;color:#888;text-align:center">'
        f'Baseline (grey tick): {COMPOSITE_BASELINE} &nbsp;·&nbsp; '
        f'Delta: <strong style="color:#c0392b">+{COMPOSITE_DELTA} pts</strong></p>',
        unsafe_allow_html=True,
    )

with col_verdict:
    st.markdown(
        f'<div class="verdict-box">{LLM_VERDICT}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<p style="font-size:10px;color:#aaa;margin-top:6px">'
        f'Generated by Rule-based verdict engine v1 · {LLM_GENERATED_AT} · '
        f'Descriptive summary only — not a prediction</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="human-bar">'
        '<strong>Human decision layer.</strong> This dashboard surfaces measured '
        'conditions and anomalies. All resource deployment and public communication '
        'decisions remain with the operator.'
        '</div>',
        unsafe_allow_html=True,
    )

st.divider()

# ── 4. INDIVIDUAL METRIC CARDS ───────────────────────────────────────────────
st.markdown("### Signal metrics")

metric_keys = list(METRICS.keys())
cols = st.columns(4)
for i, key in enumerate(metric_keys):
    m = METRICS[key]
    with cols[i % 4]:
        metric_card(
            label=key,
            value=m["value"],
            unit=m["unit"],
            delta_pct=m["delta_pct"],
            tooltip=m["tooltip"],
            docs_anchor=m["docs_anchor"],
        )

st.divider()

# ── 5. HISTORICAL CHARTS ─────────────────────────────────────────────────────
st.markdown("### Historical signal charts")

view_col, spacer = st.columns([2, 5])
with view_col:
    chart_view = st.radio(
        "Time granularity",
        ["This month · by day", "This year · by month"],
        horizontal=True,
        label_visibility="collapsed",
    )

is_year = chart_view == "This year · by month"
df    = YEAR_DATA if is_year else MONTH_DATA
x_col = "month" if is_year else "date"

st.plotly_chart(make_311_chart(df, x_col, is_year),
                use_container_width=True, config={"displayModeBar": False})

st.plotly_chart(make_env_chart(df, x_col),
                use_container_width=True, config={"displayModeBar": False})

st.plotly_chart(make_composite_chart(df, x_col),
                use_container_width=True, config={"displayModeBar": False})

# ── 6. INFRASTRUCTURE COST BREAKDOWN ─────────────────────────────────────────
st.divider()
with st.expander("☁️ AWS infrastructure cost breakdown", expanded=False):
    st.markdown(
        '<p style="font-size:12px;color:#888;margin-bottom:16px">'
        'Monthly estimate · us-east-1 · on-demand pricing · personal account 836734770581'
        '</p>',
        unsafe_allow_html=True,
    )

    # ── Summary cards ────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total / month",  "$30.93", help="us-east-1 on-demand, May 2026 pricing")
    with c2:
        st.metric("Total / year",   "$371",   help="No reserved pricing applied")
    with c3:
        st.metric("Always-on cost", "$29.53", help="RDS + ECS Fargate + ALB — billed 24/7")
    with c4:
        st.metric("Free-tier services", "5",  help="Lambda · EventBridge · Step Functions · ECR · S3 requests")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Category bar chart ───────────────────────────────────────────────────
    # go is already imported at the top of this file
    categories  = ["Dashboard (ECS + ALB)", "Database (RDS)", "Pipeline (Glue + SFN)", "Storage & Secrets", "Monitoring & Transfer"]
    costs       = [15.35, 13.98, 0.71, 0.50, 0.39]
    bar_colours = ["#534AB7", "#7F77DD", "#AFA9EC", "#CECBF6", "#B4B2A9"]

    bar_fig = go.Figure(go.Bar(
        x=costs,
        y=categories,
        orientation="h",
        marker_color=bar_colours,
        text=[f"${c:.2f}" for c in costs],
        textposition="outside",
        hovertemplate="%{y}: $%{x:.2f}/mo<extra></extra>",
    ))
    bar_fig.update_layout(
        height=200,
        margin=dict(t=10, b=10, l=10, r=60),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
        yaxis=dict(tickfont=dict(size=11), autorange="reversed"),
        showlegend=False,
        font=dict(family="system-ui,sans-serif", size=11),
    )
    st.plotly_chart(bar_fig, use_container_width=True, config={"displayModeBar": False})

    # ── Line-item tables ─────────────────────────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Dashboard**")
        st.markdown("""
| Service | Calculation | $/mo |
|---|---|---|
| ECS Fargate — vCPU | 0.25 vCPU × 730 hrs × $0.04048 | $7.39 |
| ECS Fargate — memory | 0.5 GB × 730 hrs × $0.004445 | $1.62 |
| ALB — fixed hourly | $0.008/hr × 730 hrs | $5.84 |
| ALB — LCU usage | ~0.1 LCU avg × 730 × $0.008 | $0.50 |
| ECR image storage | ~200 MB · within 500 MB free tier | free |
| **Subtotal** | | **$15.35** |
""")
        st.markdown("**ETL Pipeline**")
        st.markdown("""
| Service | Calculation | $/mo |
|---|---|---|
| Glue Python Shell | 5 jobs × 30 days × ~5 min × 0.0625 DPU | $0.69 |
| Step Functions | ~540 transitions × $0.025/1,000 | $0.01 |
| Lambda (4 functions) | ~150 invocations/mo · free tier | free |
| EventBridge rules | ~60 events/mo · free tier | free |
| **Subtotal** | | **$0.71** |
""")

    with col_b:
        st.markdown("**Database**")
        st.markdown("""
| Service | Calculation | $/mo |
|---|---|---|
| RDS db.t3.micro | $0.016/hr × 730 hrs · single-AZ | $11.68 |
| RDS gp2 storage | 20 GB × $0.115/GB | $2.30 |
| RDS automated backup | retention = 0 days | $0.00 |
| **Subtotal** | | **$13.98** |
""")
        st.markdown("**Storage, Secrets & Monitoring**")
        st.markdown("""
| Service | Calculation | $/mo |
|---|---|---|
| Secrets Manager | 1 secret × $0.40/secret | $0.40 |
| S3 storage + requests | ~250 MB scripts + GDELT bronze | $0.03 |
| Secrets Manager API | ~10k calls/mo · free tier | free |
| CloudWatch Logs | ~30 MB/mo · 1-week retention | $0.15 |
| CloudWatch Metrics | ~5 custom metrics beyond free tier | $0.15 |
| Data transfer out | ~0.5 GB/mo × $0.09/GB | $0.05 |
| **Subtotal** | | **$0.89** |
""")

    st.markdown(
        '<p style="font-size:11px;color:#aaa;margin-top:8px;border-top:0.5px solid rgba(120,120,120,0.2);padding-top:10px">'
        'Prices are us-east-1 on-demand as of May 2026. Dashboard (ECS + ALB) and Database (RDS) account for 94% of total cost — '
        'both are always-on resources billed by the hour. Pipeline, storage, and monitoring costs scale with usage and remain '
        'negligible at single-city volume.'
        '</p>',
        unsafe_allow_html=True,
    )

# ── FOOTER ───────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    '<p style="font-size:10px;color:#aaa;text-align:center">'
    'Urban Stress Intelligence Platform · '
    'Open311 GeoReport v2 · NOAA NCEI daily-summaries · GDELT GKG v2.1 · '
    'Rule-based verdict engine v1 · AWS Glue · RDS PostgreSQL'
    '</p>',
    unsafe_allow_html=True,
)