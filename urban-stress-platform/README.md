# 🏙️ Urban Stress Intelligence Platform

A daily situational awareness dashboard for Boston municipal operators — synthesising 311 service requests, NWS weather data, and GDELT media signals into a single composite stress index, served through a Streamlit dashboard on AWS ECS Fargate.

> **Live dashboard:** Deployed behind an Application Load Balancer on personal AWS account `836734770581` (us-east-1). URL available from the `DemoStack` CloudFormation output `DashboardUrl`.

---

## What it solves

Municipal emergency operations centres manage daily service demand without a unified view of city stress. Operators consult siloed dashboards — 311 volumes in one screen, weather alerts in another, news monitoring in a third. There is no single synthesised signal that answers: **"Is Boston under elevated stress today, and if so, from which direction?"**

This platform ingests three public datasets daily, computes a weighted composite stress index, and surfaces it with a rule-based contextual analysis. It is designed for EOC operators, 311 directors, and public works staff who need a single situational awareness view to make informed staffing and resource decisions.

**What it is / what it is not:**

| ✅ It is | ❌ It is not |
|---|---|
| A daily situational awareness digest | A real-time event detector |
| A composite of measured, public signals | A predictive forecasting model |
| A tool to support human judgment | A replacement for operator decisions |
| Descriptive of current conditions | An "early warning" system |

---

## Architecture

```
EventBridge cron (07:00 UTC daily)
    │
    └── Step Functions: urban-stress-etl-pipeline
            ├── Glue: bronze_etl        validates + casts raw RDS tables
            ├── Glue: silver_etl        dedup, resolution_hrs, GDELT rolling avg
            ├── Glue: signal_scoring    anomaly + heat + alert + context → composite
            ├── Glue: composite_score   8 metric cards → signal_metrics table
            ├── Glue: collector_gdelt   GDELT daily export → gdelt_daily + S3 bronze
            └── Lambda: verdict_gen     rule-based verdict → verdicts table

Lambda collectors (EventBridge, 06:00 UTC):
    ├── collector_open311  →  requests_311 table
    └── collector_noaa     →  weather_daily + nws_alerts tables

ECS Fargate (always-on):
    └── Streamlit app.py ← ALB (public DNS)
        └── reads: signal_scores, signal_metrics, verdicts, requests_311, weather_daily
```

**All infrastructure is a single CDK stack** (`cdk/stacks/demo_stack.py`) deployed to personal AWS account `836734770581`, us-east-1.

| Service | Resource | Purpose |
|---|---|---|
| S3 | `urban-stress-data-836734770581` | Glue scripts, Lambda zips, GDELT bronze |
| RDS | PostgreSQL 14, db.t3.micro | All computed metrics, scores, verdicts |
| Glue | 5 × Python Shell (GlueVersion 2.0) | ETL pipeline steps |
| Step Functions | Standard workflow | Orchestrates Glue jobs + verdict Lambda |
| EventBridge | `cron(0 7 * * ? *)` | Daily pipeline trigger |
| Lambda | `verdict_gen` (Python 3.11) | Writes rule-based verdict to RDS |
| ECS Fargate | 0.25 vCPU / 512 MB | Runs Streamlit dashboard container |
| ALB | Port 80 | Stable public URL, health check `/_stcore/health` |
| Secrets Manager | `urban-stress/db-credentials` | RDS username + password |

---

## Repository structure

```
urban-stress-platform/
│
├── app.py                          Streamlit dashboard (single file)
├── Dockerfile                      python:3.11-slim, linux/amd64
├── requirements-dashboard.txt      streamlit, psycopg2-binary, pandas, plotly
│
├── cdk/
│   ├── app.py                      CDK entry point
│   ├── requirements.txt            aws-cdk-lib, constructs
│   └── stacks/
│       └── demo_stack.py           Single CDK stack — all AWS resources
│
├── glue/
│   ├── glue_db.py                  Shared DB helper (injected via --extra-py-files)
│   ├── bronze_etl.py               Validates + casts raw source tables
│   ├── silver_etl.py               Dedup, resolution_hrs, GDELT rolling avg
│   ├── signal_scoring.py           Computes 4 signals + composite + readiness_mode
│   ├── composite_score.py          Computes 8 metric cards → signal_metrics
│   └── collector_gdelt.py          Downloads GDELT daily export, upserts gdelt_daily
│
├── lambdas/
│   ├── shared/
│   │   ├── db.py                   psycopg2 connection + upsert helper
│   │   ├── s3.py                   S3 upload helper
│   │   └── verdict_rules.py        Rule-based verdict text generator
│   ├── collector_open311/          Polls Boston Open311 API → requests_311
│   ├── collector_noaa/             Polls NWS weather.gov → weather_daily + nws_alerts
│   ├── verdict_gen/                Reads signal_scores → writes verdicts
│   └── log_run/                    Records Step Functions result → pipeline_runs
│
├── sql/
│   └── schema.sql                  Full PostgreSQL schema (all 7 tables)
│
└── scripts/
    ├── build_and_upload_lambdas.sh  Build Lambda zips (linux/amd64) + upload Glue scripts
    ├── backfill.sh                  Trigger Step Functions for N historical days
    ├── load_open311.py              One-shot 311 historical backfill (PAGE_SIZE=50)
    └── load_noaa_cdo.py             One-shot NOAA CDO historical weather backfill
```

---

## Deployment

### Prerequisites

- AWS CLI configured for account `836734770581`
- AWS CDK v2 (`npm install -g aws-cdk`)
- Docker Desktop running (for linux/amd64 image builds)
- Python 3.11+

### First-time deploy

```bash
# 1. Install CDK dependencies
cd cdk && pip install -r requirements.txt && cd ..

# 2. Deploy the full stack (builds + pushes Docker image automatically)
cd cdk && cdk deploy DemoStack --require-approval never

# 3. Upload Glue scripts + Lambda zips to S3
bash scripts/build_and_upload_lambdas.sh

# 4. Apply the database schema (get RDS endpoint from CDK output "DbEndpoint")
psql -h <DbEndpoint> -U postgres -d postgres -f sql/schema.sql

# 5. Load historical data (30 days)
export RDS_HOST=<DbEndpoint>
python scripts/load_open311.py      # backfills requests_311
python scripts/load_noaa_cdo.py     # backfills weather_daily (requires CDO_TOKEN)

# 6. Run 30-day ETL backfill (scores each historical date)
bash scripts/backfill.sh 30

# 7. Open the dashboard
# URL is in CloudFormation output: DashboardUrl
```

### Redeploy after code changes

```bash
# After editing app.py (rebuilds + pushes Docker image, updates ECS):
cd cdk && cdk deploy DemoStack --require-approval never

# After editing any Glue script (uploads to S3, no CDK needed):
bash scripts/build_and_upload_lambdas.sh

# After editing a Lambda (rebuilds zip + uploads, no CDK needed):
bash scripts/build_and_upload_lambdas.sh
```

### Environment variables

The ECS container receives these from Secrets Manager at task start:

| Variable | Source |
|---|---|
| `DB_HOST` | Injected from `urban-stress/db-credentials` secret |
| `DB_USER` | Injected from `urban-stress/db-credentials` secret |
| `DB_PASSWORD` | Injected from `urban-stress/db-credentials` secret |
| `DB_PORT` | `5432` (hardcoded in task definition) |
| `DB_NAME` | `postgres` (hardcoded in task definition) |

---

## Data sources

### Open311 Boston
**API:** `https://311.boston.gov/open311/v2/requests.json`  
**Auth:** None  
**Cadence:** Daily Lambda collection at 06:00 UTC  
**Key constraint:** Boston caps page size at 50 — requesting 100 returns a non-JSON response. `PAGE_SIZE = 50` is hardcoded in both the Lambda and backfill script.

### NWS Weather (KBOS)
**Observations API:** `https://api.weather.gov/stations/KBOS/observations`  
**Alerts API:** `https://api.weather.gov/alerts/active?area=MA`  
**Auth:** None  
**Historical backfill:** NOAA CDO API, station `GHCND:USW00014739` (Logan Airport). Requires a free token from [ncdc.noaa.gov/cdo-web/token](https://www.ncdc.noaa.gov/cdo-web/token). Set as `CDO_TOKEN` env var before running `scripts/load_noaa_cdo.py`.

### GDELT 1.0 Daily Export
**Source:** `http://data.gdeltproject.org/events/YYYYMMDD.export.CSV.zip`  
**Filter:** Events where actor geo fields match `"Boston, Massachusetts, United States"`  
**Collection:** Glue job (`glue/collector_gdelt.py`) — last step in Step Functions  
**Note:** GDELT data has a ~24 hour publication lag. The collector downloads yesterday's export.

---

## Signal definitions

The composite stress index is a weighted sum of four signals, each normalised to 0–100.

```
anomaly_score  × 0.35
heat_score     × 0.30   →   composite (0–100)   →   readiness_mode
alert_score    × 0.20
context_score  × 0.15
```

**Readiness mode** is driven by the delta from the 30-day backward-looking baseline — not the raw score — to prevent seasonal drift from triggering false alarms:

| Delta from baseline | Mode |
|---|---|
| < 10 pts | 🟢 Normal |
| 10–24 pts | 🟡 Elevated |
| ≥ 25 pts | 🔴 High |

---

## Metric reference

The `i` icons on each metric card display these descriptions on hover. The dashboard links directly to the anchors below.

---

### <a id="metric-open-backlog"></a>311 Open Backlog

**Unit:** requests  
**Source:** `COUNT(service_request_id) WHERE status = 'open' AND DATE(requested_datetime) <= obs_date`  
**Table:** `signal_metrics.open_backlog`

Total Open311 service requests with `status = 'open'` as of the observation date. Delta shows % change vs the 30-day rolling average. A rising backlog signals that demand is outpacing resolution capacity — a leading indicator of infrastructure strain before formal incident reports are filed.

---

### <a id="metric-mean-close-time"></a>Mean Close Time

**Unit:** hours  
**Source:** `AVG(updated_datetime - requested_datetime) WHERE status = 'closed' AND DATE(requested_datetime) = obs_date`  
**Table:** `signal_metrics.mean_close_hrs`

Average hours between `requested_datetime` and `updated_datetime` for requests closed on the observation date. Rising close time indicates resource strain — crews are taking longer to resolve each job, suggesting either understaffing or a backlog spillover.

---

### <a id="metric-top-category"></a>Top Complaint Category

**Unit:** service name  
**Source:** `MODE(service_name)` — highest volume `service_name` on obs_date  
**Table:** `signal_metrics.top_category`

The service category with the highest request count today. Useful for directing 311 queues and pre-positioning the relevant city agency. Persistent dominance by a single category (e.g. "Pothole" after a freeze-thaw cycle, "Needle Pickup" during a public health event) signals concentrated demand.

---

### <a id="metric-tmax"></a>TMAX Today

**Unit:** °C  
**Source:** `weather_daily.tmax` · NWS KBOS (Logan Airport)  
**Table:** `signal_metrics.tmax`

Maximum daily temperature recorded at KBOS. Used directly in the heat_score z-score calculation when heat index is unavailable (typically October–April). Delta shows % deviation from the 30-day temperature baseline.

---

### <a id="metric-heat-streak"></a>Heat Day Streak

**Unit:** consecutive days  
**Source:** `COUNT consecutive days WHERE heat_index_max > 40°C OR tmax > 35°C`, counting backward from obs_date  
**Table:** `signal_metrics.heat_streak_days`

Number of consecutive days where the NWS heat index exceeded 40°C (or raw TMAX exceeded 35°C when heat index is null). Multi-day streaks amplify physiological stress on residents and maintenance demand on infrastructure. A streak of ≥ 3 days is considered a heat event by Boston EMS protocols.

---

### <a id="metric-media-tone"></a>Media Tone (7-day avg)

**Unit:** tone score (−100 to +100)  
**Source:** `gdelt_daily.avg_tone_7d` · 7-day rolling average of GDELT AvgTone for Boston coverage  
**Table:** `signal_metrics.media_tone_7d`

7-day rolling average of GDELT's AvgTone metric for news articles mentioning Boston. Scale: −100 (extremely negative) to +100 (extremely positive). Boston's typical baseline is around −2. Values below −5 indicate elevated negative coverage. **Situational context only** — reflects events already in the public record, not future conditions.

---

### <a id="metric-protest-mentions"></a>Protest Mentions

**Unit:** events  
**Source:** `gdelt_daily.protest_event_count` · GDELT events with `EventRootCode = '14'` (PROTEST)  
**Table:** `signal_metrics.protest_mentions`

Count of GDELT-recorded events with EventRootCode `14` (Protest) that mention Boston on the observation date. Reflects media-reported protest activity already in the public record. Combined with media tone, it provides situational colour for interpreting 311 volume spikes.

---

### <a id="metric-precip"></a>Precipitation Last 72 hrs

**Unit:** mm  
**Source:** `SUM(weather_daily.precip_6hr_max)` over `obs_date - 2 days` to `obs_date`  
**Table:** `signal_metrics.precip_72hr_mm`

Total precipitation over the previous 72 hours from NWS KBOS. High values (> 25 mm) indicate storm pressure on drainage infrastructure and correlate with 311 "Catch Basin Clogged" and "Street Flooding" request spikes.

---

## Database schema

Seven tables in PostgreSQL 14. Apply with `psql ... -f sql/schema.sql`.

```
requests_311      service_request_id PK | status | requested_datetime | resolution_hrs | lat | lon
weather_daily     (station_id, obs_date) PK | tmax | tmin | heat_index_max | precip_6hr_max
nws_alerts        alert_id PK | severity | urgency | certainty | onset | expires
gdelt_daily       (city, obs_date) PK | avg_tone | avg_tone_7d | protest_event_count
signal_scores     (city, score_date) PK | anomaly_score | heat_score | alert_score | context_score | composite | readiness_mode
signal_metrics    (city, metric_date, metric_name) PK | value_num | value_text | delta_pct | baseline
verdicts          (city, verdict_date) PK | verdict_text | composite_score | readiness_mode | model_id
```

---

## Dashboard UI

The Streamlit dashboard (`app.py`) is a single-file, wide-layout app with four sections:

**1. Header** — City name, readiness mode badge (green/amber/red pill), pipeline last-run timestamp, ↻ Refresh button.

**2. Composite gauge + verdict** — Plotly gauge (0–100) with colour bands (0–40 green, 40–65 amber, 65–100 red), a grey threshold tick at the 30-day baseline, and a delta indicator. Beside it: the rule-based verdict text and a non-dismissable human decision layer notice.

**3. Signal metric cards** — 8 cards in a 4×2 grid. Each card has a value, unit, delta vs 30-day average, and an `i` button that reveals a styled tooltip explaining the metric, its source field, and how to interpret it.

**4. Historical charts** — Three Plotly charts switchable between "This month · by day" and "This year · by month":
- 311 request volume (green bars + dashed baseline)
- TMAX °C + media tone (dual Y-axis)
- Composite score vs 30-day baseline (filled area)

All fetch functions use `@st.cache_data(ttl=3600)`. The ↻ Refresh button clears the cache and reruns. The database connection is shared via `@st.cache_resource` with `autocommit = True`.

---

## ETL pipeline notes

### TARGET_DATE and backfill

All Glue scripts accept a `TARGET_DATE` argument (ISO format `YYYY-MM-DD`) to score any historical date. The argument is read via `getResolvedOptions` — **not** custom `sys.argv` parsing — because AWS Glue passes per-run override arguments through an internal mechanism that does not place them in `sys.argv`.

```python
try:
    from awsglue.utils import getResolvedOptions
    _args = getResolvedOptions(sys.argv, ["TARGET_DATE"])
    obs_date = date.fromisoformat(_args["TARGET_DATE"])
except Exception:
    obs_date = date.today() - timedelta(days=1)  # default: daily run
```

### Glue shared module

All 5 Glue scripts import `from glue_db import get_connection, upsert`. This module (`glue/glue_db.py`) is distributed to every job via the `--extra-py-files` default argument — it is not bundled in the job definition, and must be re-uploaded to S3 whenever it changes (`bash scripts/build_and_upload_lambdas.sh`).

### Glue Python Shell version

All jobs use `GlueVersion = "2.0"` (Python 3.9). Version 3.0 is only valid for Spark (`glueetl`) jobs. All code must be Python 3.9 compatible — no `X | Y` union type annotations.

---

## Cost estimate (personal account, 1 city)

| Service | ~Monthly |
|---|---|
| RDS PostgreSQL db.t3.micro | $15–20 |
| ECS Fargate (0.25 vCPU / 512 MB) | $5–8 |
| Glue Python Shell (5 jobs/day) | $2–4 |
| Lambda, S3, EventBridge, Secrets Manager | < $3 |
| **Total** | **~$25–35/month** |

No NAT Gateway (all resources in public subnets, no private egress needed), no ElastiCache, no Bedrock (rule-based verdict engine).

---

## Known constraints

- **City:** Boston, MA only. Extending to a second city requires adding a city filter parameter to all Glue scripts and Lambda collectors.
- **Open311 page size:** Boston's API silently caps at 50 records per page. `PAGE_SIZE = 100` returns a non-JSON response.
- **NWS observations window:** The NWS `/observations` endpoint only returns the last 7 days. Historical weather backfill requires the NOAA CDO API (`scripts/load_noaa_cdo.py`) with a free CDO token.
- **GDELT lag:** GDELT daily exports are published ~24 hours after the event date. The collector always runs for `obs_date = yesterday`.
- **Verdict engine:** Currently rule-based (`lambdas/shared/verdict_rules.py`). The architecture supports upgrading to Amazon Bedrock (Claude) — the verdict prompt template and RDS schema already accommodate `model_id`.

---

## License

MIT
