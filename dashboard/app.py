"""
Urban Stress Intelligence Platform — Streamlit Dashboard
=========================================================
DUMMY DATA SECTION (lines 20–130):
  All values below are hardcoded placeholders. Replace each section
  with a query to your RDS PostgreSQL datamart once ETL is complete.
  Each block is labelled with the table/view it will draw from.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random

# ─────────────────────────────────────────────────────────────────────────────
# DUMMY DATA — replace each block with a datamart query
# ─────────────────────────────────────────────────────────────────────────────

# ── Meta ─────────────────────────────────────────────────────────────────────
# Source: pipeline run log table  (pipeline_runs.last_run_at)
LAST_UPDATED = "2025-07-30 06:00 UTC"
DATA_AS_OF   = "2025-07-29"
CITY         = "Chicago, IL"

# ── Composite stress index ────────────────────────────────────────────────────
# Source: composite_scores table  (composite_scores.score, .baseline, .delta)
COMPOSITE_SCORE    = 64          # today's composite (0–100)
COMPOSITE_BASELINE = 49          # 30-day historical baseline
COMPOSITE_DELTA    = +15         # today minus baseline
READINESS_MODE     = "Elevated"  # "Normal" | "Elevated" | "High"

# LLM verdict — Source: verdicts table  (verdicts.verdict_text, .generated_at)
LLM_VERDICT = (
    "Urban stress today is **above baseline (64/100, +15 pts)**. "
    "The primary driver is environmental: this is the 4th consecutive day "
    "above 35 °C, which co-occurs historically with elevated noise and public "
    "health complaint volumes in this city. 311 noise disturbance requests are "
    "running 31% above the 30-day average. Media coverage is more negatively "
    "toned than usual — reflecting reported events, not signalling future ones. "
    "No storm pressure is present. **Operator judgment required** before "
    "adjusting staffing or resource positioning."
)
LLM_GENERATED_AT = "2025-07-30 06:04 UTC"

# ── Individual signal metrics ─────────────────────────────────────────────────
# Source: signal_metrics table  (signal_metrics.metric_name, .value, .delta_pct, .baseline)

METRICS = {
    "311 open backlog": {
        "value": 312,
        "unit": "requests",
        "delta_pct": +18.0,
        "baseline": 265,
        "source_field": "COUNT(service_request_id) WHERE status='open'",
        "tooltip": (
            "Total number of Open311 service requests with status='open' "
            "as of today. Delta shows % change vs the 30-day rolling average. "
            "A rising backlog signals demand is outpacing resolution capacity."
        ),
        "docs_anchor": "metric-open-backlog",
    },
    "Mean close time": {
        "value": 38,
        "unit": "hrs",
        "delta_pct": +30.9,
        "baseline": 29,
        "source_field": "AVG(updated_datetime - requested_datetime) WHERE status='closed'",
        "tooltip": (
            "Average hours between requested_datetime and updated_datetime "
            "for all requests closed in the last 7 days. Rising close time "
            "indicates resource strain at agency_responsible level."
        ),
        "docs_anchor": "metric-mean-close-time",
    },
    "Top complaint category": {
        "value": "Noise",
        "unit": "94 requests",
        "delta_pct": +31.0,
        "baseline": None,
        "source_field": "MODE(service_name) — today's highest volume category",
        "tooltip": (
            "The service_name with the highest request count today. "
            "Useful for directing 311 call-centre queues and pre-staffing "
            "the relevant agency. Delta vs 30-day average for that category."
        ),
        "docs_anchor": "metric-top-category",
    },
    "TMAX today": {
        "value": 37,
        "unit": "°C",
        "delta_pct": None,
        "baseline": 28,
        "source_field": "NOAA dataTypes=TMAX · dataset=daily-summaries · bbox=city",
        "tooltip": (
            "Maximum daily temperature from the nearest NOAA weather station "
            "within the city bounding box. Used to compute heat_score. "
            "Baseline is the 30-year July climatological average for this city."
        ),
        "docs_anchor": "metric-tmax",
    },
    "Heat day streak": {
        "value": 4,
        "unit": "consecutive days ≥ 35 °C",
        "delta_pct": None,
        "baseline": 0,
        "source_field": "COUNT consecutive days WHERE TMAX > 35 (NOAA daily-summaries)",
        "tooltip": (
            "Number of consecutive days where NOAA TMAX exceeded 35 °C. "
            "Streaks of 3+ days are used to trigger the elevated readiness "
            "mode and increase the heat_score weight in the composite."
        ),
        "docs_anchor": "metric-heat-streak",
    },
    "Media tone (7d avg)": {
        "value": -4.2,
        "unit": "tone score",
        "delta_pct": None,
        "baseline": -2.1,
        "source_field": "AVG(V1.5TONE.Tone) WHERE V2ENHANCEDLOCATIONS = city, window=7d",
        "tooltip": (
            "7-day rolling average of GDELT V1.5TONE.Tone for news articles "
            "mentioning this city. Scale: −100 (extremely negative) to +100 "
            "(extremely positive). Common values range −10 to +10. "
            "This is a situational context signal — it reflects what is already "
            "publicly known, not what will happen."
        ),
        "docs_anchor": "metric-media-tone",
    },
    "Protest mentions": {
        "value": 14,
        "unit": "this week",
        "delta_pct": +133.0,
        "baseline": 6,
        "source_field": "SUM(V1COUNTS WHERE CountType='PROTEST') · GDELT · last 7 days",
        "tooltip": (
            "Count of GDELT V1COUNTS records with CountType='PROTEST' and "
            "location matching this city over the past 7 days. Reflects "
            "media-reported protest activity — events already in the public "
            "record. Not a predictor of future activity."
        ),
        "docs_anchor": "metric-protest-mentions",
    },
    "Precip last 72 hrs": {
        "value": 3,
        "unit": "mm",
        "delta_pct": None,
        "baseline": 12,
        "source_field": "SUM(PRCP) WHERE startDate = today-3d · NOAA daily-summaries",
        "tooltip": (
            "Total precipitation (mm) over the previous 72 hours from NOAA "
            "daily-summaries (dataTypes=PRCP). Low values during a heat streak "
            "amplify heat stress. High values (>25 mm) trigger the infrastructure "
            "stress signal and increase infra_score weight."
        ),
        "docs_anchor": "metric-precip",
    },
}

# ── Historical time series ────────────────────────────────────────────────────
# Source: signal_history table  (signal_history.date, .signal_name, .value)

def _make_dates(n, end="2025-07-30"):
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    return [(end_dt - timedelta(days=n-1-i)).strftime("%Y-%m-%d") for i in range(n)]

random.seed(42)

# Month view — 30 daily points
MONTH_DATES = _make_dates(30)
MONTH_DATA = pd.DataFrame({
    "date":       MONTH_DATES,
    "vol_311":    [72,68,75,80,71,74,77,82,88,85,91,94,87,89,95,102,108,115,121,118,125,130,127,122,116,109,107,104,99,95],
    "base_311":   [78]*30,
    "tmax":       [29,30,31,29,32,31,33,34,35,34,36,37,35,36,38,39,38,37,39,40,38,37,36,35,34,33,34,32,31,30],
    "tone":       [-1.2,-1.4,-1.1,-1.5,-1.3,-1.6,-1.8,-2.0,-2.2,-2.1,-2.5,-2.8,-2.6,-2.9,-3.2,-3.8,-4.0,-4.2,-4.5,-4.3,-4.6,-4.8,-4.5,-4.2,-3.9,-3.5,-3.2,-3.0,-2.8,-2.5],
    "composite":  [45,43,46,48,44,45,47,50,54,52,57,60,58,61,65,71,74,77,82,80,85,90,88,84,79,74,72,70,66,64],
    "comp_base":  [49]*30,
})

# Year view — 12 monthly points
YEAR_DATA = pd.DataFrame({
    "month":      ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],
    "vol_311":    [1820,1680,2150,2280,2460,2890,3240,3010,2520,2140,1950,1790],
    "base_311":   [2160]*12,
    "tmax":       [4,6,12,17,22,28,35,34,27,19,11,5],
    "tone":       [-1.2,-1.4,-2.8,-1.9,-2.1,-2.6,-4.2,-3.8,-2.3,-1.9,-1.6,-1.3],
    "composite":  [19,17,36,40,48,62,79,73,54,40,28,18],
    "comp_base":  [20,20,35,40,48,58,70,65,50,38,28,20],
})

DOCS_BASE_URL = "https://github.com/your-org/urban-stress-platform/blob/main/docs/metrics.md"

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Urban Stress Intelligence Platform",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# THEME STATE
# ─────────────────────────────────────────────────────────────────────────────

if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

dark = st.session_state.dark_mode

# Theme colour palettes
T = {
    "app_bg":         "#0F0F1A"          if dark else "#FFFFFF",
    "surface":        "#1A1A2E"          if dark else "#FFFFFF",
    "card_bg":        "#1A1A2E"          if dark else "#FFFFFF",
    "card_border":    "rgba(255,255,255,0.08)" if dark else "rgba(120,120,120,0.2)",
    "text":           "#E0E0F0"          if dark else "#1a1a1a",
    "muted":          "#9090A8"          if dark else "#888888",
    "grid":           "#2A2A3A"          if dark else "#E8E6DF",
    "plot_bg":        "rgba(0,0,0,0)",
    "paper_bg":       "rgba(0,0,0,0)",
    "verdict_bg":     "rgba(255,255,255,0.04)" if dark else "rgba(120,120,120,0.06)",
    "human_bar_bg":   "#1C1B3A"          if dark else "#EEEDFE",
    "human_bar_bdr":  "#4A4380"          if dark else "#AFA9EC",
    "human_bar_col":  "#B0A8F0"          if dark else "#3C3489",
    "divider":        "rgba(255,255,255,0.1)" if dark else "rgba(0,0,0,0.1)",
    "link":           "#9F97E8"          if dark else "#7F77DD",
}

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL CSS  (theme-aware)
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(f"""
<style>
  /* ── App shell ── */
  .stApp {{
    background-color: {T["app_bg"]} !important;
  }}
  .block-container {{
    padding-top: 3rem;
    padding-bottom: 2rem;
    background-color: {T["app_bg"]};
  }}

  /* ── Header row vertical alignment ── */
  [data-testid="stHorizontalBlock"] > div:first-child h2 {{
    margin-top: 0.25rem;
    margin-bottom: 0;
  }}
  [data-testid="stHorizontalBlock"] > div:not(:first-child) {{
    display: flex;
    align-items: center;
    justify-content: flex-end;
  }}

  /* ── Streamlit native text elements ── */
  .stApp p, .stApp li, .stApp span, .stApp label,
  .stMarkdown p, .stMarkdown li {{
    color: {T["text"]} !important;
  }}
  .stApp h1, .stApp h2, .stApp h3,
  .stApp h4, .stApp h5, .stApp h6 {{
    color: {T["text"]} !important;
  }}

  /* ── Radio buttons ── */
  .stRadio label span {{ color: {T["text"]} !important; }}

  /* ── Dividers ── */
  hr {{ border-color: {T["divider"]} !important; }}

  /* ── Metric cards ── */
  .metric-card {{
    background: {T["card_bg"]};
    border: 0.5px solid {T["card_border"]};
    border-radius: 10px;
    padding: 14px 16px 12px;
    position: relative;
    min-height: 100px;
  }}
  .metric-label  {{ font-size: 11px; color: {T["muted"]}; text-transform: uppercase;
                   letter-spacing: .06em; margin-bottom: 4px; }}
  .metric-value  {{ font-size: 26px; font-weight: 600; line-height: 1.1;
                   color: {T["text"]}; }}
  .metric-unit   {{ font-size: 12px; color: {T["muted"]}; margin-top: 2px; }}
  .metric-delta-up   {{ color: #c0392b; font-size: 12px; font-weight: 500; }}
  .metric-delta-down {{ color: #1a7a4a; font-size: 12px; font-weight: 500; }}
  .metric-delta-neu  {{ color: {T["muted"]}; font-size: 12px; }}
  .info-icon {{
    position: absolute; top: 10px; right: 10px;
    width: 18px; height: 18px; border-radius: 50%;
    border: 1px solid {T["muted"]}; color: {T["muted"]};
    font-size: 11px; font-weight: 600;
    display: flex; align-items: center; justify-content: center;
    cursor: default; line-height: 1;
  }}

  /* ── Readiness badges — colors fixed regardless of theme ── */
  .readiness-normal   {{ background:#EAF3DE; color:#27500A !important;
                        border:0.5px solid #639922; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }}
  .readiness-elevated {{ background:#FAEEDA; color:#412402 !important;
                        border:0.5px solid #BA7517; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }}
  .readiness-high     {{ background:#FCEBEB; color:#501313 !important;
                        border:0.5px solid #E24B4A; border-radius:20px;
                        padding:4px 14px; font-size:12px; font-weight:500; }}

  /* ── Human bar & verdict ── */
  .human-bar {{
    background: {T["human_bar_bg"]}; border: 0.5px solid {T["human_bar_bdr"]};
    border-radius: 8px; padding: 10px 14px;
    font-size: 12px; color: {T["human_bar_col"]}; margin-top: 8px;
  }}
  .verdict-box {{
    background: {T["verdict_bg"]};
    border-left: 3px solid #7F77DD;
    border-radius: 0 8px 8px 0;
    padding: 12px 16px; margin-top: 10px;
    font-size: 13px; line-height: 1.7;
    color: {T["text"]};
  }}

  /* ── Theme toggle button ── */
  .theme-btn button {{
    background: {T["card_bg"]} !important;
    color: {T["text"]} !important;
    border: 1px solid {T["card_border"]} !important;
    border-radius: 20px !important;
    font-size: 12px !important;
    padding: 4px 14px !important;
  }}
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
      <div class="info-icon" title="{tooltip}">i</div>
      <div class="metric-label">{label}</div>
      <div class="metric-value">{val_str}</div>
      <div class="metric-unit">{unit}</div>
      {d_html}
    </div>
    <div style="font-size:10px;margin:4px 0 12px;padding-left:2px;">
      <a href="{docs_url}" target="_blank" style="color:{T['link']};text-decoration:none;">
        View metric docs ↗
      </a>
    </div>
    """, unsafe_allow_html=True)


def readiness_badge(mode):
    cls = f"readiness-{mode.lower()}"
    return f'<span class="{cls}">{mode} context</span>'


def chart_layout_base(title_text, height):
    """Shared layout kwargs for all charts."""
    return dict(
        title=dict(text=title_text, font=dict(size=12, color=T["muted"]), x=0),
        height=height,
        margin=dict(t=30, b=30, l=50, r=10),
        paper_bgcolor=T["paper_bg"],
        plot_bgcolor=T["plot_bg"],
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    font=dict(size=10, color=T["muted"])),
        font=dict(family="system-ui,sans-serif", color=T["text"]),
        hovermode="x unified",
    )


def composite_gauge(score, baseline):
    colour = "#27500A" if score < 40 else ("#854F0B" if score < 65 else "#A32D2D")
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=score,
        delta={"reference": baseline, "valueformat": ".0f",
               "increasing": {"color": "#c0392b"}, "decreasing": {"color": "#1a7a4a"}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1,
                     "tickcolor": T["muted"], "tickfont": {"color": T["muted"]}},
            "bar":  {"color": colour, "thickness": 0.25},
            "bgcolor": "rgba(0,0,0,0)",
            "steps": [
                {"range": [0,  40], "color": "#EAF3DE" if not dark else "#0D2010"},
                {"range": [40, 65], "color": "#FAEEDA" if not dark else "#221508"},
                {"range": [65,100], "color": "#FCEBEB" if not dark else "#230A0A"},
            ],
            "threshold": {
                "line": {"color": T["muted"], "width": 2},
                "thickness": 0.75, "value": baseline,
            },
        },
        number={"font": {"size": 40, "color": colour}},
        title={"text": "Composite stress index",
               "font": {"size": 13, "color": T["muted"]}},
    ))
    fig.update_layout(
        height=220, margin=dict(t=30, b=10, l=20, r=20),
        paper_bgcolor=T["paper_bg"], plot_bgcolor=T["plot_bg"],
        font={"family": "system-ui,sans-serif", "color": T["text"]},
    )
    return fig


def make_311_chart(df, x_col, is_year):
    fig = go.Figure()
    fig.add_bar(x=df[x_col], y=df["vol_311"], name="Requests",
                marker_color="#1D9E75", marker_opacity=0.4, marker_line_color="#1D9E75",
                marker_line_width=0.5, hovertemplate="%{y} requests<extra></extra>")
    fig.add_scatter(x=df[x_col], y=df["base_311"], name="Baseline",
                    line=dict(color="#9FE1CB", width=1.5, dash="dash"),
                    mode="lines", hovertemplate="%{y} baseline<extra></extra>")
    layout = chart_layout_base("311 request volume", 200)
    layout["yaxis"] = dict(title="req/day" if not is_year else "req/month",
                           gridcolor=T["grid"], tickfont=dict(size=10, color=T["muted"]))
    layout["xaxis"] = dict(gridcolor=T["grid"], tickfont=dict(size=10, color=T["muted"]))
    fig.update_layout(**layout)
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
    layout = chart_layout_base("Environmental & media context", 200)
    layout["margin"] = dict(t=30, b=30, l=50, r=50)
    layout["yaxis"] = dict(title=dict(text="°C", font=dict(color="#BA7517")),
                           gridcolor=T["grid"],
                           tickfont=dict(size=10, color="#BA7517"))
    layout["yaxis2"] = dict(title=dict(text="tone (0=neutral)", font=dict(color="#7F77DD")),
                            overlaying="y", side="right",
                            gridcolor="rgba(0,0,0,0)",
                            tickfont=dict(size=10, color="#7F77DD"),
                            range=[min(df["tone"])-0.5, 0.2])
    layout["xaxis"] = dict(gridcolor=T["grid"], tickfont=dict(size=10, color=T["muted"]))
    fig.update_layout(**layout)
    return fig


def make_composite_chart(df, x_col):
    fig = go.Figure()
    fig.add_scatter(x=df[x_col], y=df["comp_base"], name="Baseline",
                    line=dict(color="#B4B2A9", width=1.5, dash="dash"),
                    mode="lines",
                    hovertemplate="Baseline: %{y}<extra></extra>")
    fig.add_scatter(x=df[x_col], y=df["composite"], name="Composite score",
                    line=dict(color="#26215C" if not dark else "#9F97E8", width=2.5),
                    mode="lines+markers",
                    marker=dict(size=4, symbol="diamond"),
                    fill="tonexty", fillcolor="rgba(83,74,183,0.06)",
                    hovertemplate="Score: %{y}<extra></extra>")
    layout = chart_layout_base("Composite stress index (0–100)", 200)
    layout["yaxis"] = dict(title="score", range=[0, 100],
                           gridcolor=T["grid"],
                           tickfont=dict(size=10, color=T["muted"]))
    layout["xaxis"] = dict(gridcolor=T["grid"], tickfont=dict(size=10, color=T["muted"]))
    fig.update_layout(**layout)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. TITLE ─────────────────────────────────────────────────────────────────
col_title, col_badge, col_theme = st.columns([3, 1, 0.4])
with col_title:
    st.markdown(f"## 🏙️ Urban Stress Intelligence — {CITY}")
with col_badge:
    st.markdown(
        f'<div style="text-align:right;padding-top:14px">'
        f'{readiness_badge(READINESS_MODE)}</div>',
        unsafe_allow_html=True,
    )
with col_theme:
    st.markdown('<div style="padding-top:10px">', unsafe_allow_html=True)
    icon = "☀️ Light" if dark else "🌙 Dark"
    if st.button(icon, key="theme_toggle"):
        st.session_state.dark_mode = not st.session_state.dark_mode
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

# ── 2. UPDATED STATUS ────────────────────────────────────────────────────────
st.markdown(
    f'<p style="font-size:11px;color:{T["muted"]};margin:-6px 0 18px">'
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
        f'<p style="font-size:11px;color:{T["muted"]};text-align:center">'
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
        f'<p style="font-size:10px;color:{T["muted"]};margin-top:6px">'
        f'Generated by Amazon Bedrock (Claude 3 Sonnet) · {LLM_GENERATED_AT} · '
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

# ── FOOTER ───────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    f'<p style="font-size:10px;color:{T["muted"]};text-align:center">'
    'Urban Stress Intelligence Platform · '
    'Open311 GeoReport v2 · NOAA NCEI daily-summaries · GDELT GKG v2.1 · '
    'Amazon Bedrock · AWS Glue · RDS PostgreSQL'
    '</p>',
    unsafe_allow_html=True,
)
