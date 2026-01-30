
# Urban Stress Intelligence Platform

## Overview
This project builds an **automated, near-real-time data engineering pipeline** that collects open public data and transforms it into **city-level analytical datamarts** for detecting early warning signals of urban infrastructure and public safety stress in selected U.S. cities.

The system continuously ingests heterogeneous data streams, performs ETL to standardize and aggregate metrics, loads a warehouse-ready analytics layer, produces datamarts optimized for queries, and serves results to a minimal frontend.

---

## Problem Statement
Urban stress can be reflected early through multiple weak signals such as rising infrastructure complaints, worsening media tone, and severe weather alerts. These signals exist across independent public datasets and are difficult to interpret consistently without an integrated analytics pipeline.

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

Each signal is normalized to a **0–100 severity score** using historical baselines.  
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

## Technologies Used
- **Cloud Platform:** AWS (Academy environment)
- **Language:** Python
- **Ingestion:** scheduled Python collectors (REST + CSV pulls)
- **ETL/Processing:** managed ETL services and Python-based transforms
- **Warehouse/Query:** Athena or Redshift
- **Visualization/Frontend:** QuickSight or lightweight web frontend

---

## Repository Structure (Suggested)
```

/collectors
├── gdelt/
├── open311/
└── weather/

/etl
├── standardize/
├── normalize/
├── aggregate/
└── datamarts/

/warehouse
├── table_definitions/
└── queries/

/frontend
├── dashboard/
└── api_client/

/docs
├── proposal.md
├── evaluation.md
└── methodology.md

```

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

