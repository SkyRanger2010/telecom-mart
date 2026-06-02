"""
Сводка загрузки DDS за день: таблица dds_table_load_log (строки пишет dds_table_runner).

Запуск после всех dds_jobs. По dq_status помечает источники с WARN/FAIL/ERROR в summary_line.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from dq_common import (
    commit_trino_if_supported,
    dq_summary_phase_dds,
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


def _fetch_log_rows(
    cur,
    *,
    catalog: str,
    dds_schema: str,
    process_date: date,
) -> list[dict[str, Any]]:
    """Читает строки ``dds_table_load_log`` за указанный день.

    Args:
        cur: Курсор Trino.
        catalog: Каталог.
        dds_schema: Схема DDS.
        process_date: Календарный день (snapshot_day).

    Returns:
        Список словарей с полями журнала.
    """
    tref = quote_table(catalog, dds_schema, "dds_table_load_log")
    cur.execute(
        f"""
        SELECT source_table_fqn, dds_table, source_mode, rows_loaded,
               dq_status, dq_note
        FROM {tref}
        WHERE snapshot_day = {sql_date(process_date)}
        ORDER BY source_table_fqn
        """
    )
    cols = [
        "source_table_fqn",
        "dds_table",
        "source_mode",
        "rows_loaded",
        "dq_status",
        "dq_note",
    ]
    return [dict(zip(cols, row)) for row in (cur.fetchall() or [])]


def _persist(
    trino_target,
    catalog: str,
    validate_schema: str,
    process_date: date,
    aggregates: dict[str, Any],
    summary_line: str,
) -> None:
    """Сохраняет сводку DQ DDS в ``validate.dq_daily_summary``.

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
                phase=dq_summary_phase_dds(),
                aggregates=aggregates,
                summary_line=summary_line,
            )
        commit_trino_if_supported(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="DQ: сводка dds_table_load_log за календарный день.")
    parser.add_argument("--config", default="/opt/airflow/scripts/raw_load_config.json")
    parser.add_argument("--process-date", required=True, help="Тот же день, что cutoff-day / ds для DDS.")
    parser.add_argument("--log-level", default=os.getenv("RAW_LOADER_LOG_LEVEL", "INFO"))
    args = parser.parse_args()
    setup_logging(args.log_level)

    cfg = load_config(args.config)
    pipeline = cfg.get("pipeline", {})
    catalog = str(pipeline.get("catalog", "iceberg"))
    dds_schema = str(pipeline.get("dds_schema", "dds"))
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
            rows = _fetch_log_rows(cur, catalog=catalog, dds_schema=dds_schema, process_date=process_date)

    if not rows:
        msg = (
            f"DQ DDS: за {process_date} нет строк в dds_table_load_log — "
            f"проверьте dds_jobs ({catalog}.{dds_schema})"
        )
        LOGGER.warning("[WARN] %s", msg)
        aggregates: dict[str, Any] = {"tables": [], "totals": {"rows_loaded": 0}, "warning": "no_dds_log_rows"}
        _persist(trino_target, catalog, validate_schema, process_date, aggregates, msg)
        return

    total_loaded = sum(int(r["rows_loaded"] or 0) for r in rows)
    warn_sources: list[str] = []
    for r in rows:
        upl = str(r.get("dq_status") or "").upper()
        if "WARN" in upl or "FAIL" in upl or "ERROR" in upl:
            warn_sources.append(str(r["source_table_fqn"]))
    warn_sources = sorted(set(warn_sources))

    aggregates = {
        "tables": rows,
        "totals": {"rows_loaded": total_loaded},
        "tables_count": len(rows),
        "dq_warn_or_fail_sources": warn_sources,
    }
    summary_line = f"DQ DDS {process_date}: записей журнала={len(rows)}, sum(rows_loaded)={total_loaded}"
    if warn_sources:
        summary_line += f"; проверить dq_status: {warn_sources}"

    LOGGER.info("[INFO] %s", summary_line)
    for r in rows:
        LOGGER.info(
            "[INFO] DQ DDS detail: %s → %s mode=%s rows=%s dq=%s note=%s",
            r["source_table_fqn"],
            r["dds_table"],
            r["source_mode"],
            r["rows_loaded"],
            r["dq_status"],
            r["dq_note"],
        )

    _persist(trino_target, catalog, validate_schema, process_date, aggregates, summary_line)


if __name__ == "__main__":
    main()
