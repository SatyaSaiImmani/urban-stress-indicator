from datetime import date

SIGNAL_LABELS = {
    "anomaly_score":  "311 service demand",
    "heat_score":     "heat index",
    "alert_score":    "NWS weather alerts",
    "context_score":  "media and social context",
}

def describe_anomaly(score: float) -> str:
    if score >= 80:
        return "311 service demand is significantly above the 30-day baseline — unusually high citizen-reported activity."
    elif score >= 60:
        return "311 service demand is moderately above baseline — request volume trending upward."
    elif score <= 30:
        return "311 service demand is below baseline — quieter than usual for this period."
    else:
        return "311 service demand is within normal range."

def describe_heat(score: float) -> str:
    if score >= 80:
        return "Heat index is significantly above seasonal average — population heat stress risk is elevated."
    elif score >= 60:
        return "Heat index is above seasonal average — warmer than typical for this period."
    elif score <= 30:
        return "Heat index is below seasonal average — cooler than typical, no heat stress risk."
    else:
        return "Heat index is within normal seasonal range."

def describe_alert(score: float) -> str:
    if score >= 75:
        return "Active NWS extreme or severe weather alert in effect — declared emergency conditions."
    elif score >= 40:
        return "Active NWS moderate weather alert in effect — conditions exceed normal thresholds."
    elif score > 0:
        return "Minor NWS advisory in effect — monitor conditions."
    else:
        return "No active NWS weather alerts for the Boston area."

def describe_context(score: float) -> str:
    if score >= 70:
        return "Media tone is strongly negative with elevated protest activity — significant social tension reported."
    elif score >= 50:
        return "Media tone is moderately negative — above-average tension in news coverage."
    elif score <= 20:
        return "Media tone is neutral to positive — no unusual social tension in coverage."
    else:
        return "Media tone and protest activity are within normal range."

SIGNAL_DESCRIBERS = {
    "anomaly_score":  describe_anomaly,
    "heat_score":     describe_heat,
    "alert_score":    describe_alert,
    "context_score":  describe_context,
}

def primary_driver(scores: dict) -> str:
    neutral = {"anomaly_score": 50, "heat_score": 50, "alert_score": 0, "context_score": 50}
    deviations = {k: abs(scores[k] - neutral[k]) for k in neutral}
    return max(deviations, key=deviations.get)

def overall_statement(composite: float, baseline: float, mode: str) -> str:
    delta = composite - baseline
    if mode == "High":
        return (
            f"Urban stress conditions are significantly elevated — composite index is {composite:.1f} "
            f"({delta:+.1f} pts above the 30-day baseline of {baseline:.1f})."
        )
    elif mode == "Elevated":
        return (
            f"Urban stress conditions are moderately above baseline — composite index is {composite:.1f} "
            f"({delta:+.1f} pts above the 30-day baseline of {baseline:.1f})."
        )
    else:
        return (
            f"Urban stress conditions are within normal range — composite index is {composite:.1f} "
            f"({delta:+.1f} pts relative to the 30-day baseline of {baseline:.1f})."
        )

def build_verdict(scores: dict, obs_date: date) -> str:
    composite = scores["composite"]
    baseline  = scores["composite_baseline_30d"]
    mode      = scores["readiness_mode"]

    signal_scores = {k: scores[k] for k in SIGNAL_LABELS}
    driver = primary_driver(signal_scores)

    lines = []

    # 1. Overall statement
    lines.append(overall_statement(composite, baseline, mode))

    # 2. Primary driver callout
    lines.append(
        f"Primary driver: {SIGNAL_LABELS[driver]} ({driver.replace('_', ' ')}: {signal_scores[driver]:.1f}/100)."
    )

    # 3. Per-signal breakdown
    for key, describer in SIGNAL_DESCRIBERS.items():
        lines.append(describer(signal_scores[key]))

    # 4. Mandatory closing line
    lines.append(
        "Operator judgment required before adjusting staffing or resource positioning."
    )

    return " ".join(lines)