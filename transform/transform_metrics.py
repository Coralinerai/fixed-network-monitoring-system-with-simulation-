
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
 
logger = logging.getLogger(__name__)
 
 
# ── Data model ────────────────────────────────────────────────────────────────
 
@dataclass
class MetricRow:
    """One transformed data point.
 
    Maps directly to one row in the metrics table:
        PRIMARY KEY (device_name, metric_name, component, collected_at)
 
    Each OpenTSDB entry is one (metric, tags) combination — i.e. one component
    on one device — so every MetricRow becomes its own DB row. A device with
    16 PON components produces 16 separate MetricRows for the same metric name,
    each with a different `component` value.
 
    The full `tags` dict is kept so callers can inspect any tag that isn't
    mapped to a named field (e.g. objectType, or future tags).
    """
    device_name: str
    category: str
    metric_name: str
    component: str | None   # extracted from relativeObjectID; maps to metrics.component
    tags: dict              # full OpenTSDB tag set, preserved for reference
    collected_at: datetime
    value: float
 
    def to_tuple(self) -> tuple:
        """Return a tuple in the column order expected by upsert_metrics:
        (device_name, category, metric_name, component, collected_at, value)
        """
        return (
            self.device_name,
            self.category,
            self.metric_name,
            self.component,
            self.collected_at,
            self.value,
        )
 
 
# ── Classification ────────────────────────────────────────────────────────────
 
def classify_metric(metric_field: str) -> str | None:
    m = metric_field.lower()
    if "transceivers" in m and ("rx-power" in m or "tx-power" in m or "tx-bias" in m):
        return "OPTICAL"
    if "hardware" in m:
        return "HARDWARE"
    if "fec" in m or "bip" in m:
        return "FEC_ERRORS"
    if "dropped" in m or "discard" in m or "in-errors" in m or "out-errors" in m:
        return "DROPS"
    if "octets" in m or "pkts" in m or "packets" in m or "bytes" in m:
        return "TRAFFIC"
    return None
 
 
# ── Field parsing ─────────────────────────────────────────────────────────────
 
def parse_metric_field(metric_field: str) -> tuple[str | None, str | None]:
    """Return (metric_name, device_name) from a dotted metric string.
 
    The device name is the last segment starting with 'LS_'.
    The metric name is the segment immediately before it.
    """
    if not metric_field:
        return None, None
 
    segments = metric_field.split(".")
 
    device_index = None
    for i in range(len(segments) - 1, -1, -1):
        if segments[i].startswith("LS_"):
            device_index = i
            break
 
    if device_index is None:
        return None, None
 
    device_name = segments[device_index]
    metric_name = segments[device_index - 1] if device_index > 0 else metric_field
    return metric_name, device_name
 
 

 
def parse_component_from_tags(tags: dict, category: str) -> str | None:
    relative_id = tags.get("relativeObjectID", "")
    if not relative_id:
        return None

    if category in ("HARDWARE", "OPTICAL"):
        marker = "component__e_"
        idx = relative_id.find(marker)
        if idx == -1:
            return None
        return relative_id[idx + len(marker):]

    if category in ("FEC_ERRORS", "DROPS" , "TRAFFIC"):
        marker = "interface__e_"
        idx = relative_id.find(marker)
        if idx == -1:
            return None
        return relative_id[idx + len(marker):]

    return None
 
# ── Datapoint extraction ──────────────────────────────────────────────────────
 
def get_all_datapoints(dps: dict) -> list[tuple[datetime, float]]:
    """Return all (collected_at, value) pairs from dps."""
    result = []
    for ts, val in dps.items():
        if val is None:
            continue
        collected_at = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        result.append((collected_at, float(val)))
    return result
 
 
# ── Per-entry transform ───────────────────────────────────────────────────────
 
def transform_metric(raw: dict) -> list[MetricRow]:
    metric_field = raw.get("metric", "")
    tags = raw.get("tags", {})
    dps = raw.get("dps", {})

    category = classify_metric(metric_field)
    if category is None:
        logger.warning("Skipping metric — unrecognized category: %s", metric_field)
        return []

    metric_name, device_name = parse_metric_field(metric_field)

    if not device_name:
        logger.warning("Skipping metric — no device name: %s", metric_field)
        return []

    if not metric_name:
        logger.warning("Skipping metric — no metric name: %s", metric_field)
        return []

    component = parse_component_from_tags(tags, category)
    datapoints = get_all_datapoints(dps)

    if not datapoints:
        logger.warning("Skipping metric — no valid datapoints: %s", metric_field)
        return []

    return [
        MetricRow(
            device_name=device_name,
            category=category,
            metric_name=metric_name,
            component=component,
            tags=tags,
            collected_at=collected_at,
            value=value / 10.0 if category == "OPTICAL" else value,
        )
        for collected_at, value in datapoints
    ]
 

 
def transform_all_metrics(raw_list: list) -> list[MetricRow]:
    results: list[MetricRow] = []
    skipped = 0

    for raw in raw_list:
        if not isinstance(raw, dict):
            skipped += 1
            continue

        rows = transform_metric(raw)
        if not rows:
            skipped += 1
            continue

        results.extend(rows)

    logger.info("Transformed %d metric rows, skipped %d", len(results), skipped)
    return results