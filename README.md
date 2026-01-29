# Urban Stress Intelligence Platform

## Overview
This project implements a **real-time urban stress intelligence platform** that integrates multiple open, near-real-time public datasets to quantify early warning signals of urban infrastructure and public safety stress in selected U.S. cities.

Rather than reporting individual events or narratives, the system aggregates heterogeneous data streams into **standardized severity indicators** and a **composite Urban Stress Score**, enabling timely and interpretable situational awareness.

The project is designed as a **data engineering–focused system**, emphasizing ingestion, transformation, warehousing, and analytical querying over prediction or causal inference.

---

## Problem Statement
Urban stress often emerges gradually through multiple weak signals such as increasing infrastructure complaints, worsening media tone, or environmental stressors. These signals are typically scattered across independent public data sources and difficult to interpret in isolation.

This project addresses the question:

**How can near-real-time open public data be integrated to detect early warning signals of urban infrastructure and public safety stress within selected U.S. cities?**

---

## Key Features
- Continuous ingestion of heterogeneous open datasets
- City-level aggregation and baseline comparison
- Five standardized stress signals normalized to a 0–100 scale
- Composite Urban Stress Score for rapid assessment
- Minimal, single-page analytical interface
- Fully explainable and reproducible metrics

---

## Data Sources
The platform integrates the following open public data sources:

- **GDELT Global Knowledge Graph (GKG)**  
  Media-derived themes, tone, and location signals (CSV, ~15-minute updates)

- **Open311 Service Request APIs**  
  Citizen-reported infrastructure and service issues (JSON, near real time)

- **NOAA / National Weather Service Alerts**  
  Severe weather and environmental alerts (JSON, event-driven)

Each source contributes an independent perspective on urban conditions.

---

## System Architecture
High-level data flow:

```

External Open Data Sources
↓
Ingestion (Scheduled Python Collectors)
↓
Data Storage (Cloud Object Storage)
↓
Transformation & Aggregation (ETL Pipelines)
↓
Analytics Warehouse (Athena / Redshift)
↓
Stress Scoring Layer
↓
Application Interface (Dashboard / API)

```

The architecture is designed to be:
- Loosely coupled
- Reversible and re-runnable
- Scalable with data volume
- Resilient to schema and data evolution

---

## Stress Signals
The system computes five core stress signals for each city:

1. Media Negativity Spike  
2. Public Safety Theme Surge  
3. Infrastructure Complaint Spike  
4. Emergency-Type Complaint Ratio  
5. Weather Alert Severity  

Each signal is normalized to a **0–100 severity score** using historical baselines.  
A weighted average of these signals produces the **Urban Stress Score**.

> The system measures severity and co-occurrence of signals, not causality.

---

## Intended Users
- City operations and planning teams
- Policy analysts and urban researchers
- Journalists and media analysts
- NGOs focused on urban resilience
- Academic and educational users

The platform is intended for analytical awareness and research, not emergency response or operational decision-making.

---

## Technologies Used
- **Cloud Platform:** AWS (Academy environment)
- **Programming Language:** Python
- **Data Storage:** Cloud object storage
- **ETL & Processing:** Managed ETL services and Python pipelines
- **Analytics & Querying:** Athena or Redshift
- **Visualization:** QuickSight or lightweight web UI
- **APIs:** REST-based open data APIs

All components rely on publicly available tools and datasets.

---

## Project Structure (Logical)
```

/ingestion
├── gdelt_collector.py
├── open311_collector.py
└── weather_collector.py

/processing
├── data_cleaning.py
├── aggregation.py
└── scoring.py

/analytics
├── queries.sql
└── dashboards/

/docs
├── methodology.md
└── evaluation.md

```

---

## Team Responsibilities
- **Harshitha**
  - Data source analysis and ingestion pipelines
  - Data validation and consistency checks

- **Sri Satya Sai Immani**
  - Data modeling and aggregation logic
  - Stress signal computation and scoring framework

- **Oluwagbemi**
  - Problem framing and metric interpretation
  - Application flow, UI design, and report synthesis

---

## Evaluation Approach
The system is evaluated based on:
- Data ingestion completeness and reliability
- Timeliness of stress signal updates
- Stability and interpretability of stress indicators
- Query performance and analytical usability

The project explicitly does not evaluate prediction accuracy or causal relationships.

---

## Limitations
- The platform measures severity, not causation
- Results depend on availability and quality of open public data
- Stress scores are relative to historical baselines and not absolute measures of safety

---

## Future Extensions
- Integration of additional cities or data sources
- Predictive modeling on top of stress indicators
- Automated alerting and anomaly notifications
- Deeper drill-down into entity-level signals

---

## License
This project uses only open public data and is intended for academic and educational purposes.
