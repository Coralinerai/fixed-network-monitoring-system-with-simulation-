import logging
import psycopg2
import psycopg2.extras
from config import PG_CONN

logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(PG_CONN)


def create_tables():
    sql = """
        CREATE TABLE IF NOT EXISTS inventory (
           device_name        VARCHAR(100),
           collection_date    DATE,
           ip_address         VARCHAR(50),
           online_state       BOOLEAN,
           hardware_type      VARCHAR(100),
           software_version   VARCHAR(100),
           device_type        VARCHAR(100),
           collection_status  VARCHAR(20),
           last_seen          TIMESTAMP,
           onu_count          INTEGER DEFAULT 0,
           PRIMARY KEY (device_name, collection_date)
        );

        CREATE TABLE IF NOT EXISTS active_alarms (
            alarm_id             TEXT PRIMARY KEY,
            device_name          VARCHAR(100),
            component            VARCHAR(200),
            severity             VARCHAR(20),
            alarm_type           VARCHAR(100),
            is_service_affecting BOOLEAN,
            alarm_text           TEXT,
            raised_at            TIMESTAMP,
            raised_at_is_valid   BOOLEAN,
            acknowledged         BOOLEAN,
            ingested_at          TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS metrics (
            device_name  VARCHAR(100),
            category     VARCHAR(20),
            metric_name  VARCHAR(200),
            component    VARCHAR(100),
            collected_at TIMESTAMP,
            value        DOUBLE PRECISION,
            PRIMARY KEY (device_name, metric_name, component, collected_at)
        );

        CREATE TABLE IF NOT EXISTS simulation_state (
            id             INTEGER PRIMARY KEY DEFAULT 1,
            sim_date       DATE NOT NULL,
            current_window TIMESTAMP
        );

        INSERT INTO simulation_state (id, sim_date, current_window)
        VALUES (1, '2026-04-06', NULL)
        ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS daily_reports (
            report_date  DATE PRIMARY KEY,
            report_text  TEXT,
            generated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_inventory_date
            ON inventory(collection_date);

        CREATE INDEX IF NOT EXISTS idx_alarms_device
            ON active_alarms(device_name);

        CREATE INDEX IF NOT EXISTS idx_alarms_severity
            ON active_alarms(severity, is_service_affecting);

        CREATE INDEX IF NOT EXISTS idx_metrics_device
            ON metrics(device_name, metric_name);

        CREATE INDEX IF NOT EXISTS idx_metrics_collected_at
            ON metrics(collected_at);
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info("Tables created / verified")
    except Exception as e:
        logger.error("Failed to create tables: %s", e)
        raise
    finally:
        conn.close()


def upsert_inventory(tuples: list):
    if not tuples:
        logger.warning("No inventory data to upsert")
        return

    sql = """
        INSERT INTO inventory (
            device_name, collection_date, ip_address, online_state,
            hardware_type, software_version, device_type,
            collection_status, last_seen, onu_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_name, collection_date)
        DO UPDATE SET
            ip_address        = EXCLUDED.ip_address,
            online_state      = EXCLUDED.online_state,
            hardware_type     = EXCLUDED.hardware_type,
            software_version  = EXCLUDED.software_version,
            device_type       = EXCLUDED.device_type,
            collection_status = EXCLUDED.collection_status,
            last_seen         = EXCLUDED.last_seen;
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, tuples, page_size=500)
        logger.info("Upserted %d inventory rows", len(tuples))
    except Exception as e:
        logger.error("Failed to upsert inventory: %s", e)
        raise
    finally:
        conn.close()


def refresh_alarms(tuples: list):
    if not tuples:
        logger.warning("No alarms to upsert")
        return
    
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO active_alarms (
                        alarm_id, device_name, component, severity,
                        alarm_type, is_service_affecting, alarm_text,
                        raised_at, acknowledged
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (alarm_id) DO UPDATE SET
                        severity             = EXCLUDED.severity,
                        is_service_affecting = EXCLUDED.is_service_affecting,
                        alarm_text           = EXCLUDED.alarm_text,
                        acknowledged         = EXCLUDED.acknowledged;
                """, tuples, page_size=500)
        logger.info("Upserted %d alarm rows", len(tuples))
    except Exception as e:
        logger.error("Failed to upsert alarms: %s", e)
        raise
    finally:
        conn.close()


def upsert_metrics(tuples: list):
    if not tuples:
        logger.warning("No metrics data to upsert")
        return

    sql = """
        INSERT INTO metrics (device_name, category, metric_name, component, collected_at, value)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_name, metric_name, component, collected_at)
        DO UPDATE SET
            value    = EXCLUDED.value,
            category = EXCLUDED.category;
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, tuples, page_size=500)
        logger.info("Upserted %d metric rows", len(tuples))
    except Exception as e:
        logger.error("Failed to upsert metrics: %s", e)
        raise
    finally:
        conn.close()


def set_simulation_date(sim_date: str):
    """Update the simulation current date. format: 'YYYY-MM-DD'"""
    sql = "UPDATE simulation_state SET sim_date = %s WHERE id = 1;"
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (sim_date,))
        logger.info("Simulation date set to %s", sim_date)
    except Exception as e:
        logger.error("Failed to set simulation date: %s", e)
        raise
    finally:
        conn.close()


def set_simulation_window(window_end: str):
    """Update the current window timestamp. format: 'YYYY-MM-DDTHH:MM:SSZ'"""
    sql = "UPDATE simulation_state SET current_window = %s WHERE id = 1;"
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (window_end,))
        logger.info("Simulation window set to %s", window_end)
    except Exception as e:
        logger.error("Failed to set simulation window: %s", e)
        raise
    finally:
        conn.close()


def clear_day_data(sim_date: str):
    """Clear metrics and inventory for a given sim_date before re-simulating that day."""
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metrics WHERE DATE(collected_at) = %s", (sim_date,))
                cur.execute("DELETE FROM inventory WHERE collection_date = %s", (sim_date,))
        logger.info("Cleared metrics and inventory for %s", sim_date)
    except Exception as e:
        logger.error("Failed to clear day data: %s", e)
        raise
    finally:
        conn.close()


def save_report(date: str, report_text: str):
    """Save a daily report for the given date."""
    sql = """
        INSERT INTO daily_reports (report_date, report_text)
        VALUES (%s, %s)
        ON CONFLICT (report_date)
        DO UPDATE SET
            report_text  = EXCLUDED.report_text,
            generated_at = NOW();
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (date, report_text))
        logger.info("Report saved for %s", date)
    except Exception as e:
        logger.error("Failed to save report: %s", e)
        raise
    finally:
        conn.close()