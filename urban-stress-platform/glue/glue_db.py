"""
glue_db.py — Glue-native replacement for lambdas/shared/db.py
==============================================================
Distributed to all Glue Python Shell jobs via:
  --extra-py-files s3://{bronze_bucket}/glue-scripts/glue_db.py

Key differences from lambdas/shared/db.py:
  - Reads DB_SECRET_ARN from sys.argv (Glue job arg) not os.environ
  - Gets host/port/dbname FROM the secret itself (not separate env vars)
  - Works with psycopg2-binary (injected via --additional-python-modules)

Same public interface: get_connection() and upsert()
"""

import json
import sys

import boto3
import psycopg2
from psycopg2.extras import execute_values

_secret_cache = None  # dict or None — no | union syntax (requires Python 3.10+, Glue uses 3.9)


def _secret_id() -> str:
    """Scan sys.argv for --DB_SECRET_ARN=<value>."""
    for token in sys.argv:
        if token.startswith("--DB_SECRET_ARN="):
            return token.split("=", 1)[1]
    return "urban-stress/db-credentials"   # fallback


def _get_secret() -> dict:
    global _secret_cache
    if _secret_cache is None:
        sm = boto3.client("secretsmanager", region_name="us-east-1")
        raw = sm.get_secret_value(SecretId=_secret_id())
        _secret_cache = json.loads(raw["SecretString"])
    return _secret_cache


def get_connection():
    """Return a live psycopg2 connection using Secrets Manager credentials."""
    s = _get_secret()
    return psycopg2.connect(
        host=s["host"],
        port=int(s.get("port", 5432)),
        dbname=s.get("dbname", "postgres"),
        user=s["username"],
        password=s["password"],
        connect_timeout=10,
        sslmode="require",
    )


def upsert(conn, table: str, rows: list, conflict_cols: list) -> int:
    """INSERT … ON CONFLICT DO UPDATE for a list of dicts."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values = [[r[c] for c in cols] for r in rows]
    conflict = ", ".join(conflict_cols)
    updates  = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    )
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(rows)
