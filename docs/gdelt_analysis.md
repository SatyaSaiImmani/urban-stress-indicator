# GDELT GKG: Media-Based Urban Stress Signals

## Purpose

This section documents how GDELT v2 Global Knowledge Graph (GKG) data is used to extract near-real-time, city-level media signals related to public safety and infrastructure stress.

Target cities:
- New York City
- Chicago
- San Francisco

---

## Dataset

- **Source:** GDELT v2 GKG
- **Format:** TSV (CSV)
- **Update Frequency:** ~15 minutes
- **Granularity:** One row per news article

---

## Columns Used

| Column | Name | Usage |
|---|---|---|
| 2 | `DATE` | Article timestamp (`YYYYMMDDHHMMSS`) |
| 5 | `SOURCEURL` | Unique article identifier |
| 7 | `V2Themes` | Stress-related theme detection |
| 10 | `V2Locations` | Canonical city-level locations |

### Location Handling

Only `V2Locations` (column 10) is used for geographic attribution.

- Provides stable, structured city-level locations
- Suitable for aggregation and datamart construction
- Articles referencing multiple locations are exploded into multiple (article × city) rows

Extended location encodings were intentionally excluded to reduce parsing complexity.

---

## Stress Theme Selection

Articles are classified as stress-related if `V2Themes` contains any of the following:

- `CRISISLEX_` (all variants)
- `KILL`
- `SHOOTING`
- `ARREST`
- `CRIME`
- `FLOOD`
- `DROUGHT`
- `POWER_OUTAGE`
- `WATER_SECURITY`
- `MANMADE_DISASTER`
- `NATURAL_DISASTER`

This list targets public safety, emergency response, and infrastructure disruption.

---

## Parsed Output Schema

After filtering, exploding, and deduplication:

```text
DATE | CITY | SOURCEURL | STRESS_THEMES
20260207214500 | New York | https://www.fox5ny.com/... | KILL;CRISISLEX_T03_DEAD
20260207214500 | San Francisco | https://hotair.com/... | CRISISLEX_C05_NEED_OF_SHELTERS
20260207214500 | Chicago | https://hotair.com/... | CRISISLEX_C05_NEED_OF_SHELTERS
```
Each row represents one unique stress-related article associated with one city.

## Example Aggregation (15-Minute Slice)

Using a single GDELT GKG update window (`20260207214500`):

```text
City            Article Count
New York        9
San Francisco   1
Chicago         1
```
