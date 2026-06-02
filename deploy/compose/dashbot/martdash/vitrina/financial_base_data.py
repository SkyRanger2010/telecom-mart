"""Данные для дашборда «Финансовые показатели»: ARPU и приведённая дневная выручка по дням."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from vitrina.clickhouse_client import fetch_rows
from vitrina.client_type_filter import build_client_type_filter_options
from vitrina.financial_config import get_financial_dashboard_config
from vitrina.segment_labels import apply_segment_labels, segment_display_label
from vitrina.subscriber_base_data import (
    SubscriberBaseFilters,
    _ch_table_has_column,
    _dataset_entry,
    _daterange,
    _fq,
    _merge_selected_into_label_pairs,
    _normalize_report_date,
    _prune_client_types,
    _tariff_fallback_label,
    _to_chart_number,
    _where_dims_sql,
    parse_subscriber_base_filters,
)

_LOGGER = logging.getLogger(__name__)

_ARPU_COLOR = "rgb(251, 191, 36)"
_REVENUE_COLOR = "rgb(59, 130, 246)"

_FINANCIAL_TABLES = (
    "kpi_revenue_daily",
    "kpi_receipts_daily",
    "kpi_arpu_daily",
    "kpi_ab0_daily",
)


def _client_type_filter_available() -> bool:
    return any(_ch_table_has_column(t, "client_type") for t in _FINANCIAL_TABLES)


def _where_financial_sql(f: SubscriberBaseFilters, table: str) -> str:
    """Разрезы: тип клиента, сегмент, услуга, тариф."""
    has_ct = _ch_table_has_column(table, "client_type")
    return _where_dims_sql(
        f,
        apply_client_type=has_ct and bool(f.client_types),
        table=table,
    )


def _revenue_facts_table() -> str:
    """Таблица для списков фильтров и выручки (приоритет kpi_revenue_daily)."""
    if _ch_table_has_column("kpi_revenue_daily", "total_revenue"):
        return "kpi_revenue_daily"
    return "kpi_arpu_daily"


def _fetch_revenue_dim_pairs(
    f: SubscriberBaseFilters,
    *,
    value_expr: str,
    label_expr: str,
    group_exprs: tuple[str, ...],
    skip_dim: str,
    not_null_pred: str,
    limit: int,
) -> list[tuple[str, str]]:
    """Значения разреза с ненулевой выручкой за период (с учётом остальных фильтров)."""
    table = _revenue_facts_table()
    metric = "total_revenue" if table == "kpi_revenue_daily" else "total_revenue"
    if not _ch_table_has_column(table, metric):
        return []
    wh = _where_dims_sql(
        f,
        apply_client_type=_ch_table_has_column(table, "client_type") and bool(f.client_types),
        table=table,
        skip_segment_ids=skip_dim == "segment",
        skip_service_kinds=skip_dim == "service_kind",
        skip_tariff_ids=skip_dim == "tariff",
        skip_client_types=skip_dim == "client_type",
    )
    group_sql = ", ".join(group_exprs)
    sql = (
        f"SELECT {value_expr} AS v, {label_expr} AS lbl "
        f"FROM {_fq(table)} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}}{wh}{not_null_pred} "
        f"GROUP BY {group_sql} "
        f"HAVING sum(`{metric}`) != 0 "
        f"ORDER BY lbl "
        f"LIMIT {int(limit)}"
    )
    try:
        _cols, rows = fetch_rows(sql, parameters={"df": f.date_from, "dt": f.date_to})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("financial filter dim %s", skip_dim)
        return []
    out: list[tuple[str, str]] = []
    for row in rows:
        if not row or row[0] is None:
            continue
        v = str(row[0]).strip()
        if not v:
            continue
        lbl = str(row[1]).strip() if len(row) > 1 and row[1] is not None else v
        out.append((v, lbl or v))
    return out


def fetch_financial_filter_options(f: SubscriberBaseFilters) -> dict[str, Any]:
    """Списки фильтров по фактической выручке (kpi_revenue_daily): тип клиента, сегмент, услуга, тариф."""
    table = _revenue_facts_table()
    has_title = _ch_table_has_column(table, "client_type_title")
    label_expr = (
        "coalesce(nullIf(trim(`client_type_title`), ''), `client_type`)"
        if has_title
        else "`client_type`"
    )
    client_rows = _fetch_revenue_dim_pairs(
        f,
        value_expr="`client_type`",
        label_expr=label_expr,
        group_exprs=("`client_type`", "`client_type_title`") if has_title else ("`client_type`",),
        skip_dim="client_type",
        not_null_pred=" AND `client_type` IS NOT NULL AND trim(`client_type`) != '' ",
        limit=500,
    )
    ct_pairs = [(code, lbl or code) for code, lbl in client_rows if code]
    ct_opts = build_client_type_filter_options(ct_pairs, selected=list(f.client_types))

    segment_rows = _fetch_revenue_dim_pairs(
        f,
        value_expr="toString(`segment_id`)",
        label_expr="toString(`segment_id`)",
        group_exprs=("`segment_id`",),
        skip_dim="segment",
        not_null_pred=" AND `segment_id` IS NOT NULL ",
        limit=500,
    )
    segment_rows = apply_segment_labels(segment_rows)
    segment_rows = _merge_selected_into_label_pairs(
        segment_rows,
        [str(i) for i in f.segment_ids],
        fallback_label=segment_display_label,
    )

    service_rows = _fetch_revenue_dim_pairs(
        f,
        value_expr="`service_kind`",
        label_expr="`service_kind`",
        group_exprs=("`service_kind`",),
        skip_dim="service_kind",
        not_null_pred=" AND `service_kind` IS NOT NULL AND trim(`service_kind`) != '' ",
        limit=300,
    )
    service_rows = _merge_selected_into_label_pairs(service_rows, list(f.service_kinds))

    tariff_rows = _fetch_revenue_dim_pairs(
        f,
        value_expr="toString(`tariff_id`)",
        label_expr="anyLast(`tariff_title`)",
        group_exprs=("`tariff_id`",),
        skip_dim="tariff",
        not_null_pred=" AND `tariff_id` IS NOT NULL ",
        limit=800,
    )
    tariff_rows = _merge_selected_into_label_pairs(
        tariff_rows,
        [str(i) for i in f.tariff_ids],
        fallback_label=_tariff_fallback_label,
    )

    return {
        **ct_opts,
        "segments": [{"value": v, "label": lbl} for v, lbl in segment_rows],
        "service_kinds": [{"value": v, "label": lbl} for v, lbl in service_rows],
        "tariffs": [{"value": v, "label": lbl} for v, lbl in tariff_rows],
    }


def _fetch_arpu_indicator_daily(f: SubscriberBaseFilters) -> dict[date, float]:
    """ARPU на графике: сводный (sum выручки / sum AB0) × дней в месяце, не sum(arpu_monthly) по разрезам."""
    if _ch_table_has_column("kpi_arpu_daily", "active_clients"):
        wh = _where_financial_sql(f, "kpi_arpu_daily")
        y_expr = (
            "(sum(`total_revenue`) / nullIf(sum(`active_clients`), 0)) "
            "* toDayOfMonth(toLastDayOfMonth(`report_date`))"
        )
        table = "kpi_arpu_daily"
    else:
        wh = _where_financial_sql(f, "kpi_revenue_daily")
        y_expr = (
            "(sum(`total_revenue`) / nullIf(sum(`paying_owners`), 0)) "
            "* toDayOfMonth(toLastDayOfMonth(`report_date`))"
        )
        table = "kpi_revenue_daily"
    sql = (
        f"SELECT `report_date` AS d, {y_expr} AS y "
        f"FROM {_fq(table)} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}}{wh} "
        "GROUP BY `report_date` ORDER BY d"
    )
    _cols, rows = fetch_rows(sql, parameters={"df": f.date_from, "dt": f.date_to})
    out: dict[date, float] = {}
    for row in rows:
        if not row or row[0] is None:
            continue
        d = _normalize_report_date(row[0])
        if d is None:
            continue
        y = _to_chart_number(row[1])
        if y is None:
            continue
        out[d] = y
    return out


def _fetch_normalized_revenue_daily(f: SubscriberBaseFilters) -> dict[date, float]:
    """Приведённая дневная выручка (kpi_revenue_daily.total_revenue)."""
    wh = _where_financial_sql(f, "kpi_revenue_daily")
    sql = (
        f"SELECT `report_date` AS d, sum(`total_revenue`) AS y "
        f"FROM {_fq('kpi_revenue_daily')} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}}{wh} "
        "GROUP BY `report_date` ORDER BY d"
    )
    _cols, rows = fetch_rows(sql, parameters={"df": f.date_from, "dt": f.date_to})
    out: dict[date, float] = {}
    for row in rows:
        if not row or row[0] is None:
            continue
        d = _normalize_report_date(row[0])
        if d is None:
            continue
        y = _to_chart_number(row[1])
        if y is None:
            continue
        out[d] = y
    return out


def _build_single_line_chart(
    f: SubscriberBaseFilters,
    *,
    chart_cfg: dict[str, Any],
    series_key: str,
    series_data: dict[date, float],
    color: str,
) -> dict[str, Any]:
    series_labels = chart_cfg.get("series") or {}
    label = str(series_labels.get(series_key, series_key))
    labels_d = _daterange(f.date_from, f.date_to)
    labels = [d.isoformat() for d in labels_d]
    datasets = [
        _dataset_entry(
            key=series_key,
            label=label,
            series_data=series_data,
            labels_d=labels_d,
            color=color,
        )
    ]
    return {
        "title": chart_cfg.get("title", ""),
        "labels": labels,
        "datasets": datasets,
        "xlabel": chart_cfg.get("xlabel", "Дата"),
        "ylabel": chart_cfg.get("ylabel", ""),
        "series_keys": [series_key],
    }


def _empty_financial_chart(chart_cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": chart_cfg.get("title", ""),
        "labels": [],
        "datasets": [],
        "xlabel": chart_cfg.get("xlabel", "Дата"),
        "ylabel": chart_cfg.get("ylabel", ""),
        "series_keys": [],
    }


def build_financial_api_payload(
    f: SubscriberBaseFilters,
    params: dict[str, Any],
) -> dict[str, Any]:
    cfg = get_financial_dashboard_config()
    has_ct = _client_type_filter_available()
    f = _prune_client_types(f)
    filters = fetch_financial_filter_options(f)

    charts_cfg = cfg.get("charts") or {}
    try:
        arpu_chart = _build_single_line_chart(
            f,
            chart_cfg=charts_cfg.get("arpu") or {},
            series_key="arpu",
            series_data=_fetch_arpu_indicator_daily(f),
            color=_ARPU_COLOR,
        )
    except Exception:  # noqa: BLE001
        _LOGGER.exception("financial arpu chart")
        arpu_chart = _empty_financial_chart(charts_cfg.get("arpu") or {})
    try:
        revenue_chart = _build_single_line_chart(
            f,
            chart_cfg=charts_cfg.get("normalized_revenue") or charts_cfg.get("revenue") or {},
            series_key="normalized_revenue",
            series_data=_fetch_normalized_revenue_daily(f),
            color=_REVENUE_COLOR,
        )
    except Exception:  # noqa: BLE001
        _LOGGER.exception("financial revenue chart")
        revenue_chart = _empty_financial_chart(
            charts_cfg.get("normalized_revenue") or charts_cfg.get("revenue") or {}
        )

    return {
        "ok": True,
        "date_from": f.date_from.isoformat(),
        "date_to": f.date_to.isoformat(),
        "has_client_type_column": has_ct,
        "charts": {"arpu": arpu_chart, "normalized_revenue": revenue_chart},
        "filters": filters,
        "selected": {
            "segment_ids": [str(i) for i in f.segment_ids],
            "service_kinds": list(f.service_kinds),
            "tariff_ids": [str(i) for i in f.tariff_ids],
            "client_types": list(f.client_types),
        },
        "config": cfg,
    }


__all__ = [
    "SubscriberBaseFilters",
    "build_financial_api_payload",
    "fetch_financial_filter_options",
    "parse_subscriber_base_filters",
]
