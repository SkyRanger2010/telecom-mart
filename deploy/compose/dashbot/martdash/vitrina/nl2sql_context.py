"""Контекст NL2SQL: JSON-запрос к LLM, системный промпт, сериализация витрин ClickHouse."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from vitrina.clickhouse_client import fetch_rows, qualified_table
from vitrina.metadata import (
    MeasureRatio,
    MeasureSum,
    VITRINS,
    VitrinaDef,
)

NL2SQL_REQUEST_VERSION = "1.0"

_LOGGER = logging.getLogger(__name__)

# Кэш min/max по ключам дат фактовых витрин (без dim_tariff / dim_segment).
_SERVING_DATE_RANGE_CACHE: dict[str, tuple[str | None, str | None]] = {}

# Доп. колонки в serving (есть в ClickHouse, не всегда в VitrinaDef.dimension_cols).
_EXTRA_COLUMNS_BY_TABLE: dict[str, list[dict[str, str]]] = {
    "kpi_ab0_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_ab30_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_ab90_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_inflow_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_outflow_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_revenue_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_receipts_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
    ],
    "kpi_arpu_daily": [
        {"name": "client_type", "type": "Nullable(String)", "role": "dimension"},
        {"name": "client_type_title", "type": "Nullable(String)", "role": "label"},
        {"name": "month_start", "type": "Date", "role": "time", "note": "начало окна (справочно)"},
        {"name": "arpu_daily", "type": "Float64", "role": "measure", "agg": "avg"},
        {"name": "arpu_monthly", "type": "Float64", "role": "measure", "agg": "avg"},
    ],
}

_TABLE_HINTS: dict[str, str] = {
    "kpi_ab0_daily": "Активные абоненты на дату среза (ENABLED на report_date).",
    "kpi_ab30_daily": "Уникальные активные за 30 календарных дней включая report_date.",
    "kpi_ab90_daily": "Уникальные активные за 90 календарных дней включая report_date.",
    "kpi_arpu_daily": (
        "arpu_daily = total_revenue / active_clients (выручка за report_date); "
        "arpu_monthly = arpu_daily × дней в месяце; client_type в разрезе."
    ),
    "kpi_revenue_daily": (
        "Приведённая дневная выручка: credits ÷ дней [date_start, date_end] проводки, "
        "если report_date в интервале и заказ ENABLED; не путать с поступлениями."
    ),
    "kpi_receipts_daily": (
        "Поступления: сумма credits без нормализации по дате проводки ledger.period."
    ),
    "kpi_inflow_daily": "Приток: new_clients — новые клиенты в день активации.",
    "kpi_outflow_daily": "Отток: churned_clients — ушедшие клиенты в день expire_time.",
    "kpi_revenue_active_month": "Выручка за календарный месяц; period_end — последний день месяца.",
    "kpi_revenue_active_week": "Выручка за ISO-неделю; period_end — воскресенье.",
    "kpi_revenue_active_quarter": "Выручка за календарный квартал; period_end — последний день квартала.",
    "kpi_revenue_active_year": "Выручка за календарный год; period_end — 31.12.",
    "dim_tariff": "Справочник тарифов; не использовать для временных рядов без JOIN.",
    "dim_segment": "Иерархия направлений: segment_title = путь «родитель / … / узел»; JOIN по segment_id.",
    "сегмент": "segment_id в фактах; подпись — dim_segment.segment_title (иерархия)",
}

_GLOSSARY: dict[str, str] = {
    "АБ0": (
        "kpi_ab0_daily.active_subscribers — активные на дату; "
        "за период без «по дням» → avg(active_subscribers), не sum по дням"
    ),
    "АБ30": "kpi_ab30_daily.active_subscribers — окно 30 дней; за период → avg",
    "АБ90": "kpi_ab90_daily.active_subscribers — окно 90 дней; за период → avg",
    "ARPU": "kpi_arpu_daily: сводный = sum(total_revenue)/nullIf(sum(active_clients),0); не sum(arpu_monthly)",
    "выручка": "kpi_revenue_daily.total_revenue — приведённая дневная; kpi_receipts_daily — поступления",
    "поступления": "kpi_receipts_daily.total_receipts — сумма credits без нормализации",
    "приток": "kpi_inflow_daily.new_clients — за период sum(new_clients)",
    "отток": "kpi_outflow_daily.churned_clients — за период sum(churned_clients)",
    "приток и отток": (
        "kpi_inflow_daily + kpi_outflow_daily; «по дням» → CTE с GROUP BY report_date, "
        "JOIN по report_date, колонки `Приток` и `Отток`"
    ),
    "сравни": "несколько метрик в соседних колонках; без разности/доли, если не просили явно",
}

_FEW_SHOT_EXAMPLES: list[dict[str, str]] = [
    {
        "question": "АБ0 по дням за январь 2024",
        "sql": (
            "SELECT report_date AS `Дата`, sum(active_subscribers) AS `АБ0`\n"
            "FROM {db}.kpi_ab0_daily\n"
            "WHERE report_date BETWEEN toDate('2024-01-01') AND toDate('2024-01-31')\n"
            "GROUP BY report_date\n"
            "ORDER BY report_date\n"
            "LIMIT 500"
        ),
    },
    {
        "question": "ARPU по тарифам на последний день марта 2024",
        "sql": (
            "SELECT tariff_id AS `ID тарифа`, tariff_title AS `Тариф`,\n"
            "       arpu_daily AS `ARPU`, total_revenue AS `Выручка`, active_clients AS `Активные`\n"
            "FROM {db}.kpi_arpu_daily\n"
            "WHERE report_date = toDate('2024-03-31')\n"
            "ORDER BY arpu_daily DESC\n"
            "LIMIT 500"
        ),
    },
    {
        "question": "Сравни сумму АБ0 и приток новых клиентов по сегментам за март 2024",
        "sql": (
            "WITH ab0 AS (\n"
            "  SELECT ifNull(segment_id, toInt64(-1)) AS segment_id,\n"
            "         avg(active_subscribers) AS ab0_avg\n"
            "  FROM {db}.kpi_ab0_daily\n"
            "  WHERE report_date BETWEEN toDate('2024-03-01') AND toDate('2024-03-31')\n"
            "  GROUP BY segment_id\n"
            "), inflow AS (\n"
            "  SELECT ifNull(segment_id, toInt64(-1)) AS segment_id,\n"
            "         sum(new_clients) AS inflow_sum\n"
            "  FROM {db}.kpi_inflow_daily\n"
            "  WHERE report_date BETWEEN toDate('2024-03-01') AND toDate('2024-03-31')\n"
            "  GROUP BY segment_id\n"
            ")\n"
            "SELECT coalesce(ab0.segment_id, inflow.segment_id) AS `Сегмент`,\n"
            "       ab0.ab0_avg AS `АБ0 среднее`,\n"
            "       inflow.inflow_sum AS `Приток новых клиентов`\n"
            "FROM ab0\n"
            "FULL OUTER JOIN inflow ON ab0.segment_id = inflow.segment_id\n"
            "ORDER BY `Сегмент`\n"
            "LIMIT 500"
        ),
    },
    {
        "question": "Сравни приток и отток по дням за апрель 2025",
        "sql": (
            "WITH inflow AS (\n"
            "  SELECT report_date, sum(new_clients) AS inflow_sum\n"
            "  FROM {db}.kpi_inflow_daily\n"
            "  WHERE report_date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30')\n"
            "  GROUP BY report_date\n"
            "), outflow AS (\n"
            "  SELECT report_date, sum(churned_clients) AS outflow_sum\n"
            "  FROM {db}.kpi_outflow_daily\n"
            "  WHERE report_date BETWEEN toDate('2025-04-01') AND toDate('2025-04-30')\n"
            "  GROUP BY report_date\n"
            ")\n"
            "SELECT coalesce(inflow.report_date, outflow.report_date) AS `Дата`,\n"
            "       inflow.inflow_sum AS `Приток`,\n"
            "       outflow.outflow_sum AS `Отток`\n"
            "FROM inflow\n"
            "FULL OUTER JOIN outflow ON inflow.report_date = outflow.report_date\n"
            "ORDER BY `Дата`\n"
            "LIMIT 500"
        ),
    },
]

NL2SQL_SYSTEM_PROMPT_TEMPLATE = """Ты — генератор SQL для аналитики телеком-оператора. Диалект: ClickHouse.

## Формат ответа
- Только один блок markdown: ```sql ... ```
- Без пояснений до и после блока.
- Один оператор: WITH … SELECT или SELECT (без «;» в конце).
- Если пользователь не указал LIMIT — добавь LIMIT {max_rows} (не больше).

## Жёсткие ограничения
- Только чтение: запрещены INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, SYSTEM, KILL, OPTIMIZE, SETTINGS, SHOW, DESCRIBE, FORMAT, INTO OUTFILE.
- Используй только таблицы из JSON-поля `vitrinas` (полное имя: значение database.table_qualifier + точка + table).
- Не выдумывай колонки: только из описания витрин.
- Не используй SELECT * без необходимости; явно перечисляй поля.
- Даты: литералы toDate('YYYY-MM-DD') или колонки Date; диапазоны — BETWEEN toDate('…') AND toDate('…').
- Агрегация: active_subscribers за период (без «по дням») → avg(); приток/отток/дневная выручка → sum(); ARPU сводный → sum(total_revenue)/nullIf(sum(active_clients),0), не sum(arpu_monthly).
- «Сравни» — соседние колонки, без разности/доли, если пользователь не просил явно.

## Выбор витрины
| Смысл | Таблица | Ключ даты | Основные меры |
| АБ0 | kpi_ab0_daily | report_date | active_subscribers |
| АБ30 | kpi_ab30_daily | report_date | active_subscribers |
| АБ90 | kpi_ab90_daily | report_date | active_subscribers |
| Выручка за день | kpi_revenue_daily | report_date | total_revenue, paying_owners |
| ARPU на дату | kpi_arpu_daily | report_date | arpu_daily, total_revenue, active_clients |
| Приток | kpi_inflow_daily | report_date | new_clients, new_agreements, new_orders |
| Отток | kpi_outflow_daily | report_date | churned_clients, completed_agreements, completed_orders |
| Выручка за месяц/неделю/квартал/год | kpi_revenue_active_month/week/quarter/year | period_end | total_revenue, paying_owners |
| Тарифы (справочник) | dim_tariff | — | tariff_id, tariff_title |
| Направления (справочник) | dim_segment | — | segment_id, segment_title (иерархия) |

- Дневные ряды — фильтр по report_date.
- Периодные витрины — фильтр по period_end.
- **Период по умолчанию:** если в question нет явного календарного периода (даты, месяц/год/квартал, «за …», «с … по …», «последние N дней») — обязательно ограничь выборку полным диапазоном defaults.date_from … defaults.date_to по ключу даты выбранной витрины (report_date или period_end). Если пользователь указал период — используй его и не подставляй defaults.
- defaults.date_from / date_to — весь доступный диапазон данных в витринах (см. data_catalog.available_date_range).
- Если filters.* непусты — добавь IN (...) только для указанных списков.

## JOIN
- По умолчанию достаточно одной витрины.
- JOIN с dim_tariff только для подписи тарифа, если нет tariff_title в факте.
- JOIN с dim_segment для подписи направления (segment_title с иерархией parent_id).
- Две метрики из разных daily-витрин — CTE + FULL OUTER JOIN по ключу разреза (report_date, segment_id, …).
- Приток + отток «по дням» — две CTE (inflow/outflow), GROUP BY report_date, JOIN по report_date, колонки `Приток` / `Отток`.

## Качество
- ORDER BY для временных рядов по дате ASC.
- В финальном SELECT — русские алиасы AS `...` (backticks для кириллицы); в CTE — технические имена допустимы.
- new_clients → `Приток`, churned_clients → `Отток`.

Ниже в user-сообщении — полный JSON-контекст. Следуй полю question и структуре vitrinas."""


def _measure_column_def(m: MeasureSum | MeasureRatio) -> list[dict[str, str]]:
    if isinstance(m, MeasureSum):
        col = m.column
        if col.endswith("_daily") or "revenue" in col:
            ctype = "Float64"
        else:
            ctype = "Int64"
        return [{"name": col, "type": ctype, "role": "measure", "agg": m.agg}]
    return [
        {
            "name": m.numerator_col,
            "type": "Float64",
            "role": "measure",
            "agg": "sum",
            "note": f"числитель для {m.label_ru}",
        },
        {
            "name": m.denominator_col,
            "type": "Int64",
            "role": "measure",
            "agg": "sum",
            "note": f"знаменатель для {m.label_ru}",
        },
    ]


def serialize_vitrina(v: VitrinaDef) -> dict[str, Any]:
    """Одна витрина для JSON-контекста LLM."""
    columns: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_col(defn: dict[str, str]) -> None:
        name = defn["name"]
        if name in seen:
            return
        seen.add(name)
        columns.append(defn)

    if v.table == "dim_tariff":
        add_col({"name": "refreshed_at", "type": "DateTime64(6, 'UTC')", "role": "time"})
        add_col({"name": "tariff_id", "type": "Int64", "role": "dimension"})
        add_col({"name": "tariff_title", "type": "String", "role": "label"})
    else:
        add_col({"name": v.date_column, "type": "Date", "role": "time"})
        if v.table == "kpi_arpu_daily":
            add_col({"name": "month_start", "type": "Date", "role": "time", "note": "начало окна ARPU"})
        for d in v.dimension_cols:
            if d == "tariff_title":
                add_col({"name": d, "type": "Nullable(String)", "role": "label"})
            elif d.endswith("_id"):
                add_col({"name": d, "type": "Nullable(Int64)", "role": "dimension"})
            else:
                add_col({"name": d, "type": "Nullable(String)", "role": "dimension"})
        for m in v.measures:
            for cdef in _measure_column_def(m):
                add_col(cdef)
        for extra in _EXTRA_COLUMNS_BY_TABLE.get(v.table, ()):
            add_col(extra)

    grain_parts = [v.date_column, *v.dimension_cols]
    if v.table in _EXTRA_COLUMNS_BY_TABLE:
        grain_parts.append("client_type")

    return {
        "table": v.table,
        "title": v.title_ru,
        "description": v.description_ru,
        "date_column": v.date_column,
        "grain": " × ".join(grain_parts),
        "columns": columns,
        "hints": _TABLE_HINTS.get(v.table, ""),
    }


def serialize_all_vitrinas() -> list[dict[str, Any]]:
    return [serialize_vitrina(v) for v in VITRINS]


def build_nl2sql_system_prompt(*, max_rows: int) -> str:
    return NL2SQL_SYSTEM_PROMPT_TEMPLATE.format(max_rows=int(max_rows))


def build_nl2sql_user_message(request: dict[str, Any]) -> str:
    import json

    return (
        "Сгенерируй ClickHouse SQL по полю `question` в JSON ниже.\n\n"
        f"```json\n{json.dumps(request, ensure_ascii=False, indent=2)}\n```"
    )


def _coerce_iso_date(val: Any) -> date | None:
    if val is None:
        return None
    if hasattr(val, "date"):
        d = val.date()  # type: ignore[union-attr]
        return d if isinstance(d, date) else None
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        return date.fromisoformat(val.split(" ")[0])
    return None


def fetch_serving_data_date_range(database: str) -> tuple[str | None, str | None]:
    """Min/max по ключам дат всех фактовых витрин (кроме справочников)."""
    if database in _SERVING_DATE_RANGE_CACHE:
        return _SERVING_DATE_RANGE_CACHE[database]

    mins: list[date] = []
    maxs: list[date] = []
    for v in VITRINS:
        if v.table in {"dim_tariff", "dim_segment"}:
            continue
        fq = qualified_table(database, v.table)
        dc = v.date_column
        sql = f"SELECT min(`{dc}`) AS mn, max(`{dc}`) AS mx FROM {fq}"
        try:
            _cols, rows = fetch_rows(sql, parameters={})
        except Exception:  # noqa: BLE001
            _LOGGER.exception("nl2sql date range %s", v.table)
            continue
        if not rows:
            continue
        mn = _coerce_iso_date(rows[0][0])
        mx = _coerce_iso_date(rows[0][1])
        if mn is not None:
            mins.append(mn)
        if mx is not None:
            maxs.append(mx)

    if not mins or not maxs:
        result: tuple[str | None, str | None] = (None, None)
    else:
        result = (min(mins).isoformat(), max(maxs).isoformat())
    _SERVING_DATE_RANGE_CACHE[database] = result
    return result


def _resolve_nl2sql_default_dates(
    database: str,
    date_from: str | None,
    date_to: str | None,
) -> tuple[str | None, str | None]:
    """Если период не передан с UI — подставить весь доступный диапазон из ClickHouse."""
    if date_from and date_to:
        return date_from, date_to
    catalog_from, catalog_to = fetch_serving_data_date_range(database)
    return date_from or catalog_from, date_to or catalog_to


def build_nl2sql_request(
    question: str,
    *,
    database: str,
    max_rows: int = 500,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, list[Any]] | None = None,
    extra_examples: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Собрать JSON-контекст для llm-api /nl2sql/completions."""
    q = question.strip()
    if not q:
        raise ValueError("пустой вопрос")

    examples: list[dict[str, str]] = []
    for ex in _FEW_SHOT_EXAMPLES:
        examples.append(
            {
                "question": ex["question"],
                "sql": ex["sql"].format(db=database),
            }
        )
    if extra_examples:
        examples.extend(extra_examples)

    filt = filters or {}
    resolved_from, resolved_to = _resolve_nl2sql_default_dates(database, date_from, date_to)
    catalog_from, catalog_to = fetch_serving_data_date_range(database)

    return {
        "version": NL2SQL_REQUEST_VERSION,
        "task": "nl2sql_clickhouse",
        "locale": "ru",
        "question": q,
        "database": {
            "engine": "clickhouse",
            "name": database,
            "table_qualifier": database,
        },
        "constraints": {
            "dialect": "clickhouse",
            "only_select": True,
            "max_rows": int(max_rows),
            "forbidden": [
                "INSERT",
                "UPDATE",
                "DELETE",
                "DDL",
                "SYSTEM",
                "FORMAT",
                "INTO OUTFILE",
            ],
            "single_statement": True,
        },
        "defaults": {
            "date_from": resolved_from,
            "date_to": resolved_to,
        },
        "data_catalog": {
            "available_date_range": {
                "min": catalog_from,
                "max": catalog_to,
            },
        },
        "filters": {
            "segment_ids": list(filt.get("segment_ids") or []),
            "service_kinds": list(filt.get("service_kinds") or []),
            "tariff_ids": list(filt.get("tariff_ids") or []),
            "client_types": list(filt.get("client_types") or []),
        },
        "glossary": dict(_GLOSSARY),
        "vitrinas": serialize_all_vitrinas(),
        "examples": examples,
    }
