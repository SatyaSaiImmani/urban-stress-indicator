import os
import requests
from datetime import date, timedelta
import statistics

from s3 import write_bronze
from db import get_connection, upsert

NWS_BASE    = "https://api.weather.gov"
STATION_ID  = "KBOS"
ALERT_AREA  = "MA"

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

    # Filter out rows with no temperature reading
    rows = [r for r in rows if r["temp_c"] is not None]
    if not rows:
        return None

    def col(key):
        return [r[key] for r in rows if r[key] is not None]

    temps        = col("temp_c")
    heat_indexes = col("heat_index")
    precips      = col("precip_6hr")
    wind_gusts   = col("wind_gust")
    humidities   = col("humidity")
    descs        = col("desc")

    def mode_of(lst):
        if not lst:
            return None
        freq = {}
        for v in lst:
            freq[v] = freq.get(v, 0) + 1
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

    # Observations
    obs_features = fetch_observations(obs_date)
    print(f"  Fetched {len(obs_features)} observation records")
    weather_row = parse_observations(obs_features, obs_date)

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