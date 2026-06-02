"""
Сводка дневной загрузки RAW: читает raw_load_manifest (пишет load_raw_day).

Задача Airflow сразу после raw_load_day. В лог — итоговая строка и построчно по таблицам;
в Iceberg — одна запись в validate.dq_daily_summary (phase=raw_manifest).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

# В контейнере Airflow скрипты лежат рядом; при локальном запуске поднимаем корень airflow/scripts.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dq_common import (
    commit_trino_if_supported,
    dq_summary_phase_raw,
    ensure_dq_daily_summary_table,
    write_dq_daily_summary,
)
from load_raw_day import (
    build_trino_target_config,
    connect_trino,
    load_config,
    quote_table,
    setup_logging,
    sql_date,
)

LOGGER = logging.getLogger(__name__)


def _fetch_manifest_rows(
    cur,
    *,
    catalog: str,
    raw_schema: str,
    process_date: date,
) -> list[dict[str, Any]]:
    """Читает строки ``raw_load_manifest`` за указанный день.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        raw_schema: Схема RAW.
        process_date: Календарный день загрузки.

    Returns:
        Список словарей с полями manifest.
    """
    mref = quote_table(catalog, raw_schema, "raw_load_manifest")
    cur.execute(
        f"""
        SELECT source_table, mode, rows_read, rows_events_inserted, rows_state_upserted,
               rows_invalid, rows_snapshot_loaded, status
        FROM {mref}
        WHERE load_day = {sql_date(process_date)}
        ORDER BY source_table
        """
    )
    cols = [
        "source_table",
        "mode",
        "rows_read",
        "rows_events_inserted",
        "rows_state_upserted",
        "rows_invalid",
        "rows_snapshot_loaded",
        "status",
    ]
    rows_out: list[dict[str, Any]] = []
    for row in cur.fetchall() or []:
        rows_out.append(dict(zip(cols, row)))
    return rows_out


def _persist(
    trino_target,
    catalog: str,
    validate_schema: str,
    process_date: date,
    aggregates: dict[str, Any],
    summary_line: str,
) -> None:
    """Сохраняет сводку DQ RAW в ``validate.dq_daily_summary``.

    Args:
        trino_target: Конфигурация подключения Trino.
        catalog: Каталог.
        validate_schema: Схема DQ.
        process_date: Календарный день.
        aggregates: Словарь агрегатов.
        summary_line: Текстовая сводка.
    """
    with connect_trino(trino_target) as conn:
        with conn.cursor() as cur:
            summary_ref = ensure_dq_daily_summary_table(cur, catalog, validate_schema)
            write_dq_daily_summary(
                cur,
                summary_ref=summary_ref,
                process_date=process_date,
                phase=dq_summary_phase_raw(),
                aggregates=aggregates,
                summary_line=summary_line,
            )
        commit_trino_if_supported(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="DQ: сводка raw_load_manifest за календарный день.")
    parser.add_argument("--config", default="/opt/airflow/scripts/raw_load_config.json")
    parser.add_argument("--process-date", required=True, help="Дата партии YYYY-MM-DD (как ds)")
    parser.add_argument("--log-level", default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    setup_logging(args.log_level)

    cfg = load_config(args.config)
    pipeline = cfg.get("pipeline", {})
    catalog = str(pipeline.get("catalog", "iceberg"))
    raw_schema = str(pipeline.get("raw_schema", "raw"))
    validate_schema = str(pipeline.get("validate_schema", "validate"))

    process_date = date.fromisoformat(args.process_date)
    target_cfg = cfg.get("target", {})
    trino_target = build_trino_target_config(
        target_cfg, fallback_schema=str(target_cfg.get("default_schema", "raw"))
    )

    with connect_trino(trino_target) as conn:
        with conn.cursor() as cur:
            cur.execute(trino_target.verify_connection_sql)
            cur.fetchone()
            rows = _fetch_manifest_rows(cur, catalog=catalog, raw_schema=raw_schema, process_date=process_date)

    # Нет строк — типично, если RAW не отработал или манифест отключён; всё равно пишем сводку в Iceberg.
    if not rows:
        msg = (
            f"DQ RAW: за {process_date} нет строк в raw_load_manifest — "
            f"проверьте load_raw_day и каталог {catalog}.{raw_schema}"
        )
        LOGGER.warning("[WARN] %s", msg)
        aggregates: dict[str, Any] = {
            "tables": [],
            "totals": {
                "rows_read": 0,
                "rows_events_inserted": 0,
                "rows_state_upserted": 0,
                "rows_invalid": 0,
                "rows_snapshot_loaded": 0,
            },
            "warning": "no_manifest_rows",
        }
        _persist(trino_target, catalog, validate_schema, process_date, aggregates, msg)
        return

    totals = {
        "rows_read": 0,
        "rows_events_inserted": 0,
        "rows_state_upserted": 0,
        "rows_invalid": 0,
        "rows_snapshot_loaded": 0,
    }
    for r in rows:
        totals["rows_read"] += int(r["rows_read"] or 0)
        totals["rows_events_inserted"] += int(r["rows_events_inserted"] or 0)
        totals["rows_state_upserted"] += int(r["rows_state_upserted"] or 0)
        totals["rows_invalid"] += int(r["rows_invalid"] or 0)
        totals["rows_snapshot_loaded"] += int(r["rows_snapshot_loaded"] or 0)

    bad_status = [r["source_table"] for r in rows if str(r.get("status") or "").lower() != "success"]
    aggregates = {
        "tables": rows,
        "totals": totals,
        "tables_count": len(rows),
        "non_success_tables": bad_status,
    }
    summary_line = (
        f"DQ RAW {process_date}: таблиц в манифесте={len(rows)}; "
        f"sum(rows_read)={totals['rows_read']}, events={totals['rows_events_inserted']}, "
        f"state_upsert={totals['rows_state_upserted']}, invalid={totals['rows_invalid']}, "
        f"snapshot={totals['rows_snapshot_loaded']}"
    )
    if bad_status:
        summary_line += f"; ВНИМАНИЕ status!=success: {bad_status}"

    LOGGER.info("[INFO] %s", summary_line)
    for r in rows:
        LOGGER.info(
            "[INFO] DQ RAW detail: %s mode=%s read=%s ev=%s st=%s inv=%s snap=%s status=%s",
            r["source_table"],
            r["mode"],
            r["rows_read"],
            r["rows_events_inserted"],
            r["rows_state_upserted"],
            r["rows_invalid"],
            r["rows_snapshot_loaded"],
            r["status"],
        )

    _persist(trino_target, catalog, validate_schema, process_date, aggregates, summary_line)


if __name__ == "__main__":
    main()
