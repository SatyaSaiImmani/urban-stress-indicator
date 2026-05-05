import os
import requests
from datetime import date, timedelta
import statistics

from s3 import write_bronze
from db import get_connection, upsert

NWS_BASE    = "https://api.weather.gov"
STATION_ID  = "KBOS"
ALERT_AREA  = "MA"

# NOAA CDO (fallback when NWS observations unavailable — NWS only keeps ~7 days)
CDO_BASE    = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
CDO_STATION = "GHCND:USW00014739"  # Boston Logan Airport
CDO_TOKEN   = os.environ.get("CDO_TOKEN", "")

HEADERS = {
    "User-Agent": "urban-stress-platform/1.0 (immanisri.satyasai2001@gmail.com)",
    "Accept":     "application/geo+json"
}

def fetch_observations(obs_date: date) -> list:
    url = f"{NWS_BASE}/stations/{STATION_ID}/observations"
    params = {
        "start": f"{obs_date}T00:00:00Z",
        "end":   f"{obs_date}T23:59:59Z",
        "limit": 500,
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("features", [])

def fetch_alerts(obs_date: date) -> list:
    url = f"{NWS_BASE}/alerts"
    params = {
        "area":  ALERT_AREA,
        "start": f"{obs_date}T00:00:00Z",
        "end":   f"{obs_date}T23:59:59Z",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("features", [])

def fetch_cdo_fallback(obs_date: date) -> dict | None:
    """Fetch daily summary from NOAA CDO when NWS observations aren't available."""
    if not CDO_TOKEN:
        return None
    params = {
        "datasetid":  "GHCND",
        "stationid":  CDO_STATION,
        "datatypeid": "TMAX,TMIN,PRCP,AWND,WSF5",
        "startdate":  str(obs_date),
        "enddate":    str(obs_date),
        "units":      "metric",
        "limit":      20,
    }
    try:
        r = requests.get(CDO_BASE, headers={"token": CDO_TOKEN}, params=params, timeout=30)
        r.raise_for_status()
        results = r.json().get("results", [])
        values  = {item["datatype"]: float(item["value"]) for item in results}
        if "TMAX" not in values or "TMIN" not in values:
            return None
        prcp    = values.get("PRCP")
        wind    = values.get("WSF5") or values.get("AWND")
        tmax, tmin = values["TMAX"], values["TMIN"]
        desc = "Rain" if prcp and prcp > 5 else ("Hot" if tmax > 32 else ("Freezing" if tmax < 0 else "Clear"))
        return {
            "station_id":       STATION_ID,
            "obs_date":         str(obs_date),
            "tmax":             tmax,
            "tmin":             tmin,
            "heat_index_max":   None,
            "precip_6hr_max":   prcp,
            "wind_gust_max":    wind,
            "humidity_avg":     None,
            "text_description": desc,
        }
    except Exception as e:
        print(f"  CDO fallback failed: {e}")
        return None

def parse_observations(features: list, obs_date: date) -> dict:
    rows = []
    for f in features:
        p = f.get("properties", {})
        def val(field):
            v = p.get(field)
            return v.get("value") if isinstance(v, dict) else None
        rows.append({
            "temp_c":     val("temperature"),
            "heat_index": val("heatIndex"),
            "precip_6hr": val("precipitationLast6Hours"),
            "wind_gust":  val("windGust"),
            "humidity":   val("relativeHumidity"),
            "desc":       p.get("textDescription"),
        })

    rows = [r for r in rows if r["temp_c"] is not None]
    if not rows:
        return None

    def col(key):
        return [r[key] for r in rows if r[key] is not None]

    temps = col("temp_c")
    heat_indexes = col("heat_index")
    precips      = col("precip_6hr")
    wind_gusts   = col("wind_gust")
    humidities   = col("humidity")
    descs        = col("desc")

    def mode_of(lst):
        if not lst: return None
        freq = {}
        for v in lst: freq[v] = freq.get(v, 0) + 1
        return max(freq, key=freq.get)

    return {
        "station_id":       STATION_ID,
        "obs_date":         str(obs_date),
        "tmax":             max(temps),
        "tmin":             min(temps),
        "heat_index_max":   max(heat_indexes) if heat_indexes else None,
        "precip_6hr_max":   max(precips)      if precips      else None,
        "wind_gust_max":    max(wind_gusts)   if wind_gusts   else None,
        "humidity_avg":     statistics.mean(humidities) if humidities else None,
        "text_description": mode_of(descs),
    }

def parse_alerts(features: list, obs_date: date) -> list:
    rows = []
    for f in features:
        p = f.get("properties", {})
        rows.append({
            "alert_id":      f.get("id"),
            "city":          "Boston",
            "obs_date":      str(obs_date),
            "event_type":    p.get("event"),
            "severity":      p.get("severity"),
            "urgency":       p.get("urgency"),
            "certainty":     p.get("certainty"),
            "response_type": p.get("response"),
            "category":      p.get("category"),
            "onset":         p.get("onset"),
            "expires":       p.get("expires"),
            "headline":      p.get("headline"),
            "description":   p.get("description"),
            "instruction":   p.get("instruction"),
            "area_desc":     p.get("areaDesc"),
        })
    return [r for r in rows if r["alert_id"]]

def handler(event=None, context=None):
    obs_date = date.today() - timedelta(days=1)
    print(f"Fetching NWS data for {obs_date}")

    # Observations (NWS — real-time, ~7 day retention)
    obs_features = fetch_observations(obs_date)
    print(f"  Fetched {len(obs_features)} observation records")
    weather_row = parse_observations(obs_features, obs_date)

    # CDO fallback if NWS had no data (shouldn't happen for yesterday, but safety net)
    if not weather_row:
        print(f"  NWS returned no observations — trying CDO fallback...")
        weather_row = fetch_cdo_fallback(obs_date)
        if weather_row:
            print(f"  CDO fallback succeeded: tmax={weather_row['tmax']:.1f}°C")

    # Alerts
    alert_features = fetch_alerts(obs_date)
    print(f"  Fetched {len(alert_features)} alert records")
    alert_rows = parse_alerts(alert_features, obs_date)

    if os.environ.get("SKIP_S3") != "true":
        write_bronze("noaa/observations", obs_features, obs_date)
        write_bronze("noaa/alerts", alert_features, obs_date)
        print(f"  Written to S3 bronze")

    if os.environ.get("SKIP_DB") != "true":
        conn = get_connection()
        if weather_row:
            upsert(conn, "weather_daily", [weather_row], ["station_id", "obs_date"])
            print(f"  Upserted 1 row to weather_daily")
        if alert_rows:
            n = upsert(conn, "nws_alerts", alert_rows, ["alert_id"])
            print(f"  Upserted {n} rows to nws_alerts")
        else:
            print(f"  No active alerts for {obs_date}")
        conn.close()

    return {"status": "ok", "date": str(obs_date),
            "observations": len(obs_features), "alerts": len(alert_features)}

if __name__ == "__main__":
    result = handler()
    print(result)
