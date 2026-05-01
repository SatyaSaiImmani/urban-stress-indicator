CREATE TABLE pipeline_runs (
  id              SERIAL PRIMARY KEY,
  source          TEXT NOT NULL,        -- 'open311' | 'noaa' | 'gdelt'
  run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status          TEXT NOT NULL,        -- 'success' | 'failed'
  records_loaded  INT,
  error_message   TEXT
);

CREATE TABLE requests_311 (
  service_request_id  TEXT PRIMARY KEY,
  service_code        TEXT,
  service_name        TEXT,
  department          TEXT,             -- service_code.split(':')[0]
  category            TEXT,             -- service_code.split(':')[1]
  status              TEXT,             -- 'open' | 'closed'
  requested_datetime  TIMESTAMPTZ,
  updated_datetime    TIMESTAMPTZ,
  resolution_hrs      NUMERIC(8,2),     -- NULL if status = 'open'
  lat                 DOUBLE PRECISION,
  lon                 DOUBLE PRECISION,
  neighbourhood       TEXT,             -- derived via spatial join
  description         TEXT,
  loaded_at           TIMESTAMPTZ DEFAULT NOW()
);


CREATE INDEX idx_311_date ON requests_311 (requested_datetime);
CREATE INDEX idx_311_status ON requests_311 (status);
CREATE INDEX idx_311_service ON requests_311 (service_name);

CREATE TABLE weather_daily (
  station_id          TEXT,
  obs_date            DATE,
  tmax                NUMERIC(5,2),
  tmin                NUMERIC(5,2),
  heat_index_max      NUMERIC(5,2),
  precip_6hr_max      NUMERIC(7,2),
  wind_gust_max       NUMERIC(6,2),
  humidity_avg        NUMERIC(5,2),
  text_description    TEXT,
  PRIMARY KEY (station_id, obs_date)
);

CREATE TABLE nws_alerts (
  alert_id        TEXT PRIMARY KEY,
  city            TEXT NOT NULL,
  obs_date        DATE NOT NULL,
  event_type      TEXT,
  severity        TEXT,
  urgency         TEXT,
  certainty       TEXT,
  response_type   TEXT,
  category        TEXT,
  onset           TIMESTAMPTZ,
  expires         TIMESTAMPTZ,
  headline        TEXT,
  description     TEXT,
  instruction     TEXT,
  area_desc       TEXT
);

CREATE INDEX idx_alerts_date ON nws_alerts (obs_date);
CREATE INDEX idx_alerts_city ON nws_alerts (city, obs_date);

CREATE TABLE gdelt_daily (
  city                    TEXT,
  obs_date                DATE,
  avg_tone                NUMERIC(6,3),
  avg_tone_7d             NUMERIC(6,3),
  polarity                NUMERIC(6,4),
  protest_event_count     INT,
  protest_mention_sum     INT,
  goldstein_avg           NUMERIC(5,2),
  num_articles            INT,
  source_count            INT,
  PRIMARY KEY (city, obs_date)
);

CREATE TABLE signal_scores (
  city                    TEXT,
  score_date              DATE,
  anomaly_score           NUMERIC(5,2),
  heat_score              NUMERIC(5,2),
  alert_score             NUMERIC(5,2),
  context_score           NUMERIC(5,2),
  composite               NUMERIC(5,2),
  composite_baseline_30d  NUMERIC(5,2),
  readiness_mode          TEXT,
  PRIMARY KEY (city, score_date)
);

CREATE TABLE signal_metrics (
  city          TEXT,
  metric_date   DATE,
  metric_name   TEXT,
  value_num     NUMERIC,
  value_text    TEXT,
  delta_pct     NUMERIC(6,2),
  baseline      NUMERIC,
  PRIMARY KEY (city, metric_date, metric_name)
);

CREATE TABLE verdicts (
  city             TEXT,
  verdict_date     DATE,
  verdict_text     TEXT,
  composite_score  NUMERIC(5,2),
  readiness_mode   TEXT,
  generated_at     TIMESTAMPTZ,
  model_id         TEXT,
  PRIMARY KEY (city, verdict_date)
);