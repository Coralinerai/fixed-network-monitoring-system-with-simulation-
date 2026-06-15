import logging
import hashlib
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

EPOCH_THRESHOLD = datetime(1971, 1, 1, tzinfo=timezone.utc)

#alarm_id on hash , randomly 
def generate_alarm_id(raw: dict) -> str:
    """
    Generate a stable unique ID from the alarm's identifying fields.
    Uses mainDeviceRefId + alarmType + alarmResource so the same alarm
    always gets the same ID across runs.
    Falls back to _es_id if present (ES export format).
    """
    if raw.get("_es_id"):
        return raw["_es_id"]

    unique_string = "|".join([
        raw.get("mainDeviceRefId", ""),
        raw.get("alarmType", ""),
        raw.get("alarmResource", ""),
    ])
    return hashlib.md5(unique_string.encode()).hexdigest()


def parse_component_from_ui_name(alarm_resource_ui_name: str) -> str | None:
    """
    Extract component from alarmResourceUiName.

    Examples:
        "component:LS_POINT_DENIS:Alarm-Input-Port-2"  -> "Alarm-Input-Port-2"
        "lag:POL-LS-FX-1.IHUB:1"                       -> "1"
        "exportingProcess:POL-LS-FX-1:AP-ipfix-..."    -> "AP-ipfix-..."
        "license-key:Altiplano Core"                    -> None
    """
    if not alarm_resource_ui_name:
        return None

    parts = alarm_resource_ui_name.split(":")

    # format is "resource_type:device_name:component"
    if len(parts) >= 3:
        return parts[2]

    return None


def parse_timestamp(ts_string: str) -> datetime | None:
    """
    Parse ISO 8601 timestamp string to timezone-aware datetime.
    Returns None if string is empty or unparseable.
    """
    if not ts_string:
        return None
    try:
        return datetime.fromisoformat(ts_string.replace("Z", "+00:00"))
    except (ValueError, AttributeError) as e:
        logger.warning(f"Could not parse timestamp '{ts_string}': {e}")
        return None



def transform_alarm(raw: dict) -> tuple | None:
    """
    Transform one raw alarm document into a clean tuple.

    Output tuple column order matches active_alarms table:
        (alarm_id, device_name, component, severity, alarm_type,
         is_service_affecting, alarm_text, raised_at, raised_at_is_valid, acknowledged)
    """

    # 1. generate stable unique id
    alarm_id = generate_alarm_id(raw)

    # 2. device_name always from mainDeviceRefId (always the root OLT)
    device_name = raw.get("mainDeviceRefId") or "PLATFORM"

    # 3. component from alarmResourceUiName — third segment after splitting on ":"
    component = parse_component_from_ui_name(raw.get("alarmResourceUiName", ""))

    # 4. alarm type is required
    alarm_type = raw.get("alarmType")
    if not alarm_type:
        logger.warning(f"Skipping alarm {alarm_id} — no alarmType")
        return None

    severity         = raw.get("alarmSeverity")
    sa_raw           = raw.get("serviceAffecting", "")
    alarm_text       = raw.get("alarmText", "")
    acknowledged_raw = raw.get("acknowledged", "false")

    # 5. raised_at from raisedTime only
    #    lastStatusChangeTime not needed — these are active alarms, not historical
    raised_at          = parse_timestamp(raw.get("raisedTime"))
    

    is_service_affecting = (sa_raw == "SA_SERVICE_AFFECTING")
    acknowledged         = str(acknowledged_raw).lower() == "true"

    return (
        alarm_id,
        device_name,
        component,
        severity,
        alarm_type,
        is_service_affecting,
        alarm_text,
        raised_at,
        acknowledged
    )


def transform_all_alarms(raw_list: list) -> list:
    results = []
    skipped = 0

    for raw in raw_list:
        row = transform_alarm(raw)
        if row is None:
            skipped += 1
            continue
        results.append(row)

    logger.info(f"Transformed {len(results)} alarms, skipped {skipped}")
    return results
