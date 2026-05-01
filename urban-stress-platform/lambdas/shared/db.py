import os
import json
import boto3
import psycopg2
from psycopg2.extras import execute_values

_secret_cache: dict | None = None

def _get_secret() -> dict:
    global _secret_cache
    if _secret_cache is None:
        client = boto3.client(
            "secretsmanager",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        raw = client.get_secret_value(SecretId=os.environ["DB_SECRET_ARN"])
        _secret_cache = json.loads(raw["SecretString"])
    return _secret_cache

def get_connection():
    secret = _get_secret()
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=secret["username"],
        password=secret["password"],
    )

def upsert(conn, table: str, rows: list[dict], conflict_cols: list[str]):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values = [[r[c] for c in cols] for r in rows]
    conflict = ", ".join(conflict_cols)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in conflict_cols
    )
    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES %s
        ON CONFLICT ({conflict}) DO UPDATE SET {updates}
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
    return len(rows)