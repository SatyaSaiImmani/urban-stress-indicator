import json
from pathlib import Path

raw = Path("/D/sxi219/sxi219_onedrive/My Documents/CSDS417/urban-stress-indicator/collectors/open311/data/raw/boston/2026-03-14/faff2830-e905-4309-921e-db97c6c8949e/response.json")
bronze = Path("/D/sxi219/sxi219_onedrive/My Documents/CSDS417/urban-stress-indicator/collectors/open311/data/bronze/year=2026/month=03/day=14/city=boston/batch_id=faff2830-e905-4309-921e-db97c6c8949e/part-00001.jsonl")

with raw.open() as f:
    raw_count = len(json.load(f))

with bronze.open() as f:
    bronze_count = sum(1 for _ in f)

print("raw_count   =", raw_count)
print("bronze_count=", bronze_count)
print("match       =", raw_count == bronze_count)