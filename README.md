
# Urban Stress Intelligence Platform

## Overview
This project builds an **automated, near-real-time data engineering pipeline** that collects open public data and transforms it into **city-level analytical datamarts** to detect early warning signals of urban infrastructure and public safety stress in selected U.S. cities.

The system continuously ingests heterogeneous data streams, performs ETL to standardize and aggregate metrics, loads a warehouse-ready analytics layer, produces datamarts optimized for queries, and serves results to a minimal frontend.

---

## Contributing
We welcome contributions from the community. This section explains the expected workflow and quality bar.

### Before You Start
- Check existing issues and PRs to avoid duplicate work
- For non-trivial changes, open an issue describing the problem and your approach
- Keep changes scoped; prefer small, focused PRs

### Setup
1. Fork the repository
2. Create a new branch: `git checkout -b feature/short-description`
3. Install dependencies with uv: `uv sync`

### Make Changes
- Follow the existing code style and conventions
- Add or update tests for new behavior
- Update documentation if behavior or outputs change

### Test
- Run relevant tests or scripts for the area you changed
- If no tests exist, describe what you validated in the PR

### Commit and Open a PR
1. Commit with a clear message: `git commit -m "Describe the change"`
2. Push your branch: `git push origin feature/short-description`
3. Open a pull request with:
   - What changed and why
   - How you tested
   - Any follow-up work or risks

### Code Standards
- Use meaningful variable and function names
- Comment complex logic appropriately
- Follow PEP 8 guidelines for Python code
- Keep functions and classes focused on a single responsibility

### Reporting Issues
- Use the issue tracker to report bugs or suggest enhancements
- Provide detailed information about the problem or suggestion
- Include steps to reproduce bugs when possible
- Be respectful and constructive in all interactions

### Questions?
If you have questions about contributing, feel free to open an issue for discussion or mail me at sxi219@case.edu.

## Problem Statement
Urban stress can surface early through weak signals such as rising infrastructure complaints, worsening media tone, and severe weather alerts. These signals exist across independent public datasets and are difficult to interpret consistently without an integrated analytics pipeline.

### Research Question
**How can near-real-time open public data be integrated to detect early warning signals of urban infrastructure and public safety stress within selected U.S. cities?**

---

## Core Output
The platform produces:
- **5 standardized stress signals** (0–100 severity each)
- **1 composite Urban Stress Score** summarizing overall severity
- **City-level datamarts** enabling fast analytics and visualization

> The system measures severity and co-occurrence of indicators. It does not infer causality or validate event truth.

---

## Data Sources
- **GDELT v2 GKG** (CSV, ~15-minute updates): media tone and themes tied to locations
- **Open311 APIs** (JSON, near real time): infrastructure/service request signals per city
- **NOAA / NWS Alerts** (JSON, event-driven): weather hazard context and severity

---

## Automated Pipeline
End-to-end automated flow:

```

External Open Data Sources
↓
Ingestion (Scheduled Collectors)
↓
ETL (Standardize → Parse → Aggregate)
↓
Data Warehouse (Analytics Query Layer)
↓
Datamarts (City-Level Signals + Baselines)
↓
Frontend (Single-Page City Stress View)

```

### Key Pipeline Characteristics
- Automated, scheduled ingestion and ETL runs
- Repeatable transformations (re-runnable for any time window)
- Datamarts optimized for fast queries
- Designed to tolerate schema and data evolution over time

---

## Stress Signals
The datamarts compute five city-level stress signals:

1. **Media Negativity Spike (GDELT)**
2. **Public Safety Theme Surge (GDELT)**
3. **Infrastructure Complaint Spike (Open311)**
4. **Emergency-Type Complaint Ratio (Open311)**
5. **Weather Alert Severity (NOAA/NWS)**

Each signal is normalized to a **0-100 severity score** using historical baselines.
A weighted average of the five signals produces the **Urban Stress Score**.

---

## Datamarts (What the Frontend Queries)
The frontend does not query raw ingested data. It queries curated datamarts such as:
- **Hourly/Daily city signal metrics**
- **Hourly/Daily baseline metrics (rolling historical windows)**
- **Composite score outputs**
- **Signal contribution breakdowns**

These datamarts support fast visualization and consistent interpretation.

---

## Intended Users
- City operations and planning teams (situational awareness)
- Policy analysts and urban researchers (trend analysis)
- Journalists and media analysts (coverage and stress indicators)
- NGOs focused on resilience and community support
- Academic users (data engineering and open-data analytics)

---

## Pipeline Map (End-to-End)

### 1) External Open Data Sources
- **GDELT v2 GKG** (near real-time media signals)
- **Open311 APIs** (city service/infrastructure requests)
- **NOAA / NWS Alerts** (weather and hazard alerts)

### 2) Orchestration / Scheduling
- **Apache Airflow** schedules and monitors the pipeline runs
  - ingestion DAGs (per source)
  - transformation + aggregation DAGs
  - scoring + datamart refresh DAGs

### 3) Ingestion
- **Python collectors** fetch/pull data from each source
- Data is written to **AWS S3** (or course Hadoop storage if S3 is unavailable)
  - stored with ingestion timestamp
  - partitioned by `source` and `date`

### 4) Storage (Durable Data Lake Layout)
- **Raw data**: unchanged source payloads (CSV/JSON) for traceability
- **Processed data**: standardized and cleaned datasets
- **Analytics datasets**: aggregated city/time tables optimized for queries

### 5) ETL / Distributed Processing
- **Apache Spark** performs:
  - schema normalization (timestamps, city mapping)
  - parsing required fields (themes/tone, 311 categories, alert severity)
  - aggregations (hourly/daily metrics per city)
  - baseline computation (rolling historical windows)

### 6) Analytics / Warehouse Query Layer
Choose one based on availability:
- **Amazon Athena** (SQL over S3 Parquet), or
- **Amazon Redshift** (loaded warehouse tables), or
- **Spark SQL** over Parquet (if Athena/Redshift are not available)

### 7) Datamarts (What the App Queries)
Curated query-ready tables such as:
- City × Time aggregated metrics (media, 311, weather)
- Baseline statistics (mean/std for rolling windows)
- 5 normalized signal scores (0–100 severity)
- Composite Urban Stress Score (weighted aggregation)

### 8) Serving Layer (Optional but Recommended)
- **API Gateway + AWS Lambda** exposes endpoints to query datamarts
  - `/score?city=...&window=...`
  - `/signals?city=...&window=...`
- Enables the frontend to stay decoupled from the warehouse engine

### 9) Frontend (Minimal UI)
- Single-page dashboard showing:
  - City selector + time window
  - Composite Urban Stress Score
  - 5 signal scores (0–100) with simple severity labels

### 10) Monitoring & Reliability
- Airflow task logs + run history
- Basic data quality checks (row counts, null checks, drift alerts)
- Retry + failure handling for ingestion/ETL steps


---

## Team Responsibilities
- **Harshitha**
  - Data source analysis and ingestion pipelines
  - Data validation and consistency checks

- **Sri Satya Sai Immani**
  - Data aggregation design and datamart modeling
  - Stress signal computation and scoring framework

- **Oluwagbemi**
  - Problem framing and metric interpretation
  - Frontend flow, presentation, and report synthesis

---

## Evaluation
The system will be evaluated on:
- **Reliability:** ingestion completeness and ETL correctness
- **Timeliness:** end-to-end update latency
- **Analytical stability:** baseline consistency and anomaly behavior
- **Usability:** clarity and interpretability of the single-page output
- **Performance:** query latency on curated datamarts

---

## Limitations
- Measures severity and co-occurrence, not causality
- Dependent on availability/quality of open data APIs
- Stress score is relative to historical baselines, not an absolute measure of safety

---

## License
Academic project using open public data sources. See individual data providers for terms of use.
