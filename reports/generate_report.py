"""
Standalone network health report generator.
Uses separate database calls for each section to avoid cursor issues.
"""
import sys
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def query(sim_date: str, sql: str, params: tuple = None) -> list:
    """Execute a single query and return all rows."""
    from load.pg_loader import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or (sim_date,))
            return cur.fetchall() or []
    except Exception as e:
        logger.warning("Query failed: %s", e)
        return []
    finally:
        conn.close()


def query_one(sim_date: str, sql: str, params: tuple = None) -> int:
    """Execute a single query and return first value of first row."""
    from load.pg_loader import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or (sim_date,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return row[0]
            return 0
    except Exception as e:
        logger.warning("Query failed: %s", e)
        return 0
    finally:
        conn.close()


def generate_report(sim_date: str) -> str:
    """Generate a full daily network health report for the given date."""

    # ── fetch all data ────────────────────────────────────────────────────
    inventory = query(sim_date, """
        SELECT device_name, online_state, onu_count,
               ip_address, hardware_type, software_version
        FROM inventory WHERE collection_date = %s ORDER BY device_name
    """)

    total_alarms = query_one(sim_date, """
        SELECT COUNT(*) FROM active_alarms WHERE DATE(raised_at) <= %s
    """)

    service_affecting = query_one(sim_date, """
        SELECT COUNT(*) FROM active_alarms
        WHERE DATE(raised_at) <= %s AND is_service_affecting = true
    """)

    alarm_types = query(sim_date, """
        SELECT alarm_type, COUNT(*) FROM active_alarms
        WHERE DATE(raised_at) <= %s
        GROUP BY alarm_type ORDER BY COUNT(*) DESC
    """)

    new_alarms = query(sim_date, """
        SELECT device_name, alarm_type, severity, is_service_affecting, alarm_text
        FROM active_alarms WHERE DATE(raised_at) = %s
        ORDER BY is_service_affecting DESC, severity
    """)

    cpu = query(sim_date, """
        SELECT device_name, AVG(value), MIN(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'HARDWARE' AND metric_name LIKE '%cpu-idle%'
        GROUP BY device_name ORDER BY AVG(value)
    """)

    memory = query(sim_date, """
        SELECT device_name,
               MAX(CASE WHEN metric_name LIKE '%free-memory%'  THEN value END),
               MAX(CASE WHEN metric_name LIKE '%total-memory%' THEN value END)
        FROM metrics WHERE DATE(collected_at) = %s AND category = 'HARDWARE'
        GROUP BY device_name
    """)

    dead_ports = query(sim_date, """
        SELECT device_name, component, MIN(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'OPTICAL' AND metric_name LIKE '%rx-power%'
        GROUP BY device_name, component HAVING MIN(value) = -400
        ORDER BY device_name, component
    """)

    active_ports = query(sim_date, """
        SELECT device_name, component, AVG(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'OPTICAL' AND metric_name LIKE '%rx-power%'
        AND value > -400
        GROUP BY device_name, component ORDER BY AVG(value)
    """)

    top_drops = query(sim_date, """
        SELECT device_name, component, SUM(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'DROPS' AND value > 0
        GROUP BY device_name, component ORDER BY SUM(value) DESC LIMIT 10
    """)

    traffic_in = query(sim_date, """
        SELECT device_name, MAX(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'TRAFFIC' AND metric_name LIKE '%in-octets%'
        GROUP BY device_name ORDER BY MAX(value) DESC
    """)

    traffic_out = query(sim_date, """
        SELECT device_name, MAX(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'TRAFFIC' AND metric_name LIKE '%out-octets%'
        GROUP BY device_name ORDER BY MAX(value) DESC
    """)

    fec = query(sim_date, """
        SELECT device_name, metric_name, SUM(value)
        FROM metrics WHERE DATE(collected_at) = %s
        AND category = 'FEC_ERRORS' AND value > 0
        GROUP BY device_name, metric_name ORDER BY SUM(value) DESC LIMIT 5
    """)

    metrics_coverage = query(sim_date, """
        SELECT category, COUNT(*) FROM metrics
        WHERE DATE(collected_at) = %s
        GROUP BY category ORDER BY COUNT(*) DESC
    """)

    # ── build report ──────────────────────────────────────────────────────
    lines = []
    lines.append("=" * 70)
    lines.append(f"  DAILY NETWORK HEALTH REPORT — {sim_date}")
    lines.append("=" * 70)

    # device status
    lines.append("\n━━━ DEVICE STATUS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if inventory:
        for row in inventory:
            lines.append(f"  {row[0]}")
            lines.append(f"    Status   : {'ONLINE ✓' if row[1] else 'OFFLINE ✗'}")
            lines.append(f"    IP       : {row[3] or 'N/A'}")
            lines.append(f"    Hardware : {row[4] or 'N/A'} | Software: {row[5] or 'N/A'}")
            lines.append(f"    ONUs     : {row[2] or 0}")
    else:
        lines.append("  No inventory data for this day.")

    # alarms
    lines.append("\n━━━ ALARMS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  Total active alarms  : {total_alarms}")
    lines.append(f"  Service affecting    : {service_affecting}")

    if alarm_types:
        lines.append("\n  Breakdown by type:")
        for row in alarm_types:
            lines.append(f"    {str(row[0]):<45} {row[1]:>4} alarm(s)")

    if new_alarms:
        lines.append(f"\n  New alarms raised on {sim_date}:")
        for row in new_alarms:
            sa_str = "⚠ SERVICE AFFECTING" if row[3] else "non-service-affecting"
            lines.append(f"    [{str(row[2]).upper()}] {row[0]} — {row[1]} ({sa_str})")
            if row[4] and row[4] != "N.A":
                lines.append(f"      → {str(row[4])[:120]}")
    else:
        lines.append(f"\n  No new alarms raised on {sim_date}.")

    # hardware
    lines.append("\n━━━ HARDWARE RESOURCES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if cpu:
        lines.append("  CPU Usage:")
        for row in cpu:
            if row[1] is not None and row[2] is not None:
                usage  = 100 - float(row[1])
                peak   = 100 - float(row[2])
                status = "⚠ HIGH" if peak > 80 else "OK"
                lines.append(f"    {row[0]}: avg {usage:.1f}% | peak {peak:.1f}% [{status}]")
            else:
                lines.append(f"    {row[0]}: CPU data unavailable")
    else:
        lines.append("  No CPU data for this day.")

    if memory:
        lines.append("  Memory:")
        for row in memory:
            free_mem  = row[1]
            total_mem = row[2]
            if free_mem is not None and total_mem is not None and float(total_mem) > 0:
                used_pct = ((float(total_mem) - float(free_mem)) / float(total_mem)) * 100
                lines.append(f"    {row[0]}: {float(free_mem)/1024:.0f} MB free / {float(total_mem)/1024:.0f} MB total ({used_pct:.1f}% used)")
            elif free_mem is not None:
                lines.append(f"    {row[0]}: {float(free_mem)/1024:.0f} MB free")

    # optical
    lines.append("\n━━━ OPTICAL HEALTH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if dead_ports:
        lines.append(f"  Dead PON ports (no signal): {len(dead_ports)}")
        for row in dead_ports[:5]:
            lines.append(f"    ✗ {row[0]} / {row[1]}")
        if len(dead_ports) > 5:
            lines.append(f"    ... and {len(dead_ports) - 5} more")
    else:
        lines.append("  No dead PON ports detected.")

    if active_ports:
        lines.append(f"  Active PON ports: {len(active_ports)}")
        for row in active_ports[:5]:
            if row[2] is not None:
                status = "GOOD" if -27 <= float(row[2]) <= -8 else "WEAK"
                lines.append(f"    {row[0]} / {row[1]}: {float(row[2]):.1f} dBm [{status}]")

    # drops
    lines.append("\n━━━ PACKET DROPS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if top_drops:
        lines.append("  Top interfaces by drops:")
        for row in top_drops:
            if row[2] is not None:
                lines.append(f"    {row[0]} / {row[1]}: {int(float(row[2])):,} drops")
    else:
        lines.append("  No drop activity today.")

    # traffic
    lines.append("\n━━━ TRAFFIC SUMMARY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if traffic_in:
        lines.append("  Peak inbound traffic (in-octets):")
        for row in traffic_in:
            if row[1] is not None:
                lines.append(f"    {row[0]}: {float(row[1]) / 1e9:.2f} GB")
    if traffic_out:
        lines.append("  Peak outbound traffic (out-octets):")
        for row in traffic_out:
            if row[1] is not None:
                lines.append(f"    {row[0]}: {float(row[1]) / 1e9:.2f} GB")

    # fec
    lines.append("\n━━━ FEC / BIP ERRORS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if fec:
        for row in fec:
            if row[2] is not None:
                lines.append(f"  {row[0]} / {row[1]}: {int(float(row[2])):,}")
    else:
        lines.append("  No FEC/BIP errors today.")

    # metrics coverage
    lines.append("\n━━━ METRICS COLLECTED TODAY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    if metrics_coverage:
        for row in metrics_coverage:
            lines.append(f"  {str(row[0]):<15} {row[1]:>8,} datapoints")
    else:
        lines.append("  No metrics collected today.")

    lines.append("\n" + "=" * 70)
    return "\n".join(lines)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_report.py YYYY-MM-DD")
        sys.exit(1)

    from load.pg_loader import save_report

    date_str = sys.argv[1]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
        sys.exit(1)

    report = generate_report(date_str)
    save_report(date_str, report)
    print(report)
    print(f"\nReport saved to daily_reports table for {date_str}")