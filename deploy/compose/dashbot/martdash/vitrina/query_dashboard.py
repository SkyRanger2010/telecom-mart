"""Дашборд «по запросу»: llm-api (DeepSeek) → sqlguard → материализация CH → визуализация."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from django.conf import settings

from vitrina.clickhouse_client import execute_commands, fetch_rows_raw, qualified_table
from vitrina.metadata import schema_nl2sql_block
from vitrina.nl2sql_context import build_nl2sql_request
from vitrina.result_viz import build_viz_payload, empty_result_fields, serialize_rows

_LOGGER = logging.getLogger(__name__)


def _assistant_from_llm_response(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        return ""
    msg = (choices[0] or {}).get("message") or {}
    return str(msg.get("content") or "")


def query_dashboard_pipeline_sync(
    question: str,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, list[Any]] | None = None,
    request_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """llm-api → sqlguard (SELECT + materialize_sql) → ClickHouse → чтение dashboard_query_stub."""
    llm_base = settings.LLM_API_URL.rstrip("/")
    sg_base = settings.SQLGUARD_API_URL.rstrip("/")
    db = settings.CLICKHOUSE_DATABASE
    max_rows = settings.NL2SQL_MAX_ROWS
    stub_table = settings.QUERY_DASHBOARD_TABLE

    if request_override is not None:
        nl_request = dict(request_override)
        if not str(nl_request.get("question") or "").strip():
            nl_request["question"] = question.strip()
    else:
        nl_request = build_nl2sql_request(
            question,
            database=db,
            max_rows=max_rows,
            date_from=date_from,
            date_to=date_to,
            filters=filters,
        )

    llm_payload: dict[str, Any] = {
        "request": nl_request,
        "model": settings.NL2SQL_LLM_MODEL,
        "question": nl_request.get("question", question),
        "schema_block": schema_nl2sql_block(db=db),
    }

    assistant_raw = ""
    try:
        lr = httpx.post(
            f"{llm_base}/api/v1/nl2sql/completions",
            json=llm_payload,
            timeout=120.0,
        )
        lr.raise_for_status()
        assistant_raw = _assistant_from_llm_response(lr.json())
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("llm-api недоступен")
        return {"ok": False, "error": f"llm-api: {ex}", "nl2sql_request": nl_request}

    if not assistant_raw.strip():
        return {
            "ok": False,
            "error": "llm-api вернул пустой ответ",
            "assistant_raw": assistant_raw,
            "nl2sql_request": nl_request,
        }

    try:
        gr = httpx.post(
            f"{sg_base}/api/v1/nl2sql/guard-assistant",
            json={
                "assistant_text": assistant_raw,
                "max_rows": max_rows,
                "materialize_table": stub_table,
                "database": db,
            },
            timeout=60.0,
        )
        gr.raise_for_status()
        guard = gr.json()
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("sqlguard-api недоступен")
        return {
            "ok": False,
            "error": f"sqlguard-api: {ex}",
            "assistant_raw": assistant_raw,
            "nl2sql_request": nl_request,
        }

    if not guard.get("ok"):
        return {
            "ok": False,
            "error": "; ".join(guard.get("errors") or []) or "sqlguard отклонил ответ модели",
            "sql_candidate": guard.get("sql_extracted"),
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    materialize_sql = list(guard.get("materialize_sql") or [])
    source_sql = guard.get("sql") or ""
    if not materialize_sql:
        return {
            "ok": False,
            "error": "sqlguard не вернул SQL материализации",
            "sql": source_sql,
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    try:
        execute_commands(materialize_sql)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("материализация dashboard_query_stub")
        return {
            "ok": False,
            "error": f"ClickHouse (материализация): {ex}",
            "sql": source_sql,
            "materialize_sql": materialize_sql,
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    read_sql = f"SELECT * FROM {qualified_table(db, stub_table)} LIMIT {max_rows}"
    try:
        columns, rows = fetch_rows_raw(read_sql)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("чтение dashboard_query_stub")
        return {
            "ok": False,
            "error": f"ClickHouse (чтение {stub_table}): {ex}",
            "sql": source_sql,
            "materialize_sql": materialize_sql,
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    return {
        "ok": True,
        "sql": source_sql,
        "materialize_sql": materialize_sql,
        "read_sql": read_sql,
        "table_name": stub_table,
        "columns": columns,
        "rows": rows,
        "warnings": list(guard.get("warnings") or []),
        "assistant_raw": assistant_raw,
        "guard_detail": guard,
        "nl2sql_request": nl_request,
    }


def _debug_block(pipeline_result: dict[str, Any]) -> dict[str, Any]:
    guard = pipeline_result.get("guard_detail")
    if not isinstance(guard, dict):
        guard = {}
    return {
        "nl2sql_request": pipeline_result.get("nl2sql_request"),
        "assistant_raw": pipeline_result.get("assistant_raw") or "",
        "guard_detail": guard,
        "sql": pipeline_result.get("sql") or pipeline_result.get("sql_candidate") or "",
        "sql_candidate": pipeline_result.get("sql_candidate"),
        "materialize_sql": pipeline_result.get("materialize_sql") or guard.get("materialize_sql"),
        "read_sql": pipeline_result.get("read_sql"),
        "table_name": pipeline_result.get("table_name"),
        "warnings": list(pipeline_result.get("warnings") or []),
    }


def load_query_dashboard_payload(
    *,
    question: str,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, list[Any]] | None = None,
    request_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Полный цикл: JSON → llm-api → sqlguard → материализация таблицы → визуализация."""
    nl = query_dashboard_pipeline_sync(
        question,
        date_from=date_from,
        date_to=date_to,
        filters=filters,
        request_override=request_override,
    )
    debug = _debug_block(nl)

    if not nl.get("ok"):
        return {
            "ok": False,
            "error": nl.get("error") or "запрос не выполнен",
            "question": question,
            **debug,
        }

    columns = list(nl.get("columns") or [])
    raw_rows = nl.get("rows") or []
    if not raw_rows:
        return {
            "ok": True,
            "question": question,
            **debug,
            **empty_result_fields(),
            "columns": columns,
            "rows": [],
            "row_count": 0,
            "sql": nl.get("sql") or "",
            "table_name": nl.get("table_name") or settings.QUERY_DASHBOARD_TABLE,
        }

    serialized_rows = serialize_rows(raw_rows)
    viz = build_viz_payload(columns, serialized_rows)

    return {
        "ok": True,
        "question": question,
        **debug,
        **viz,
        "columns": columns,
        "rows": serialized_rows,
        "row_count": len(raw_rows),
        "table_name": nl.get("table_name") or settings.QUERY_DASHBOARD_TABLE,
    }


# Обратная совместимость (если где-то импортировали старое имя).
load_stub_dashboard_payload = load_query_dashboard_payload
