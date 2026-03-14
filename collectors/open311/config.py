from __future__ import annotations

from pathlib import Path
import yaml


CONFIG_PATH = Path(__file__).resolve().parent / "cities.yaml"


def load_city_config(city_id: str) -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    for city in cfg.get("cities", []):
        if city["city_id"] == city_id:
            return city

    raise ValueError(f"City config not found for: {city_id}")
