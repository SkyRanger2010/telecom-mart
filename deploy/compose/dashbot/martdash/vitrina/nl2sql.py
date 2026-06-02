"""NL2SQL: оркестрация llm-api (генерация) + sqlguard-api (проверка ответа), выполнение в ClickHouse."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from django.conf import settings

from vitrina.clickhouse_client import fetch_rows_raw
from vitrina.metadata import schema_nl2sql_block
from vitrina.nl2sql_context import build_nl2sql_request
from vitrina.result_viz import build_viz_payload, empty_result_fields, serialize_row

_LOGGER = logging.getLogger(__name__)


def _assistant_from_llm_response(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        return ""
    msg = (choices[0] or {}).get("message") or {}
    return str(msg.get("content") or "")


def nl2sql_request_sync(
    question: str,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, list[Any]] | None = None,
    request_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """1) llm-api (NL→SQL); 2) sqlguard-api; 3) ClickHouse SELECT после guard."""

    llm_base = settings.LLM_API_URL.rstrip("/")
    sg_base = settings.SQLGUARD_API_URL.rstrip("/")
    db = settings.CLICKHOUSE_DATABASE
    max_rows = settings.NL2SQL_MAX_ROWS

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
        # обратная совместимость со старым llm-api
        "question": nl_request.get("question", question),
        "schema_block": schema_nl2sql_block(db=db),
    }

    assistant_raw = ""

    def _llm_error_message(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            try:
                body = exc.response.json()
                detail = body.get("detail") if isinstance(body, dict) else body
            except Exception:
                detail = (exc.response.text or "").strip()
            if detail:
                return f"llm-api HTTP {exc.response.status_code}: {detail}"
        return f"llm-api: {exc}"

    try:
        lr = httpx.post(
            f"{llm_base}/api/v1/nl2sql/completions",
            json=llm_payload,
            timeout=120.0,
        )
        lr.raise_for_status()
        lm_body = lr.json()
        assistant_raw = _assistant_from_llm_response(lm_body)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("llm-api недоступен")
        return {"ok": False, "error": _llm_error_message(ex), "nl2sql_request": nl_request}

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
            },
            timeout=30.0,
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

    final_sql = guard.get("sql") or ""
    warns = list(guard.get("warnings") or [])
    if not final_sql.strip():
        return {
            "ok": False,
            "error": "sqlguard не вернул SQL",
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    try:
        cols, rows = fetch_rows_raw(final_sql)
    except Exception as ex:  # noqa: BLE001
        _LOGGER.exception("ошибка ClickHouse по NL-SQL")
        return {
            "ok": False,
            "error": f"ClickHouse: {ex}",
            "sql": final_sql,
            "sql_candidate": final_sql,
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
        }

    if len(rows) > max_rows:
        rows = rows[:max_rows]

    serialized_rows = [serialize_row(r) for r in rows]
    if not serialized_rows:
        return {
            "ok": True,
            "sql": final_sql,
            "columns": cols,
            "rows": [],
            "row_count": 0,
            "warnings": warns,
            "assistant_raw": assistant_raw,
            "guard_detail": guard,
            "nl2sql_request": nl_request,
            **empty_result_fields(),
        }

    return {
        "ok": True,
        "sql": final_sql,
        "columns": cols,
        "rows": serialized_rows,
        "row_count": len(serialized_rows),
        "warnings": warns,
        "assistant_raw": assistant_raw,
        "guard_detail": guard,
        "nl2sql_request": nl_request,
        **build_viz_payload(cols, serialized_rows),
    }
