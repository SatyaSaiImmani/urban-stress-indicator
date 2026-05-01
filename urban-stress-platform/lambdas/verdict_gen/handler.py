import os
from datetime import date, timedelta

from db import get_connection, upsert
from verdict_rules import build_verdict

def fetch_scores(conn, obs_date: date) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT city, score_date,
                   anomaly_score, heat_score, alert_score, context_score,
                   composite, composite_baseline_30d, readiness_mode
            FROM signal_scores
            WHERE city = 'Boston' AND score_date = %s
        """, [str(obs_date)])
        row = cur.fetchone()
        if not row:
            return None
        cols = [
            "city", "score_date",
            "anomaly_score", "heat_score", "alert_score", "context_score",
            "composite", "composite_baseline_30d", "readiness_mode"
        ]
        return dict(zip(cols, row))

def handler(event=None, context=None):
    obs_date = date.today() - timedelta(days=1)
    print(f"Generating verdict for {obs_date}")

    conn = get_connection()

    scores = fetch_scores(conn, obs_date)
    if not scores:
        print(f"  No signal scores found for {obs_date} — skipping verdict")
        conn.close()
        return {"status": "skipped", "reason": "no signal scores"}

    verdict_text = build_verdict(scores, obs_date)

    print(f"\n--- VERDICT ---\n{verdict_text}\n---------------\n")

    row = {
        "city":            "Boston",
        "verdict_date":    str(obs_date),
        "verdict_text":    verdict_text,
        "composite_score": scores["composite"],
        "readiness_mode":  scores["readiness_mode"],
        "generated_at":    "NOW()",
        "model_id":        "rule-based-v1",
    }

    upsert(conn, "verdicts", [row], ["city", "verdict_date"])
    conn.close()

    print(f"  Verdict written to verdicts table")
    return {"status": "ok", "date": str(obs_date), "readiness_mode": scores["readiness_mode"]}

if __name__ == "__main__":
    result = handler()
    print(result)