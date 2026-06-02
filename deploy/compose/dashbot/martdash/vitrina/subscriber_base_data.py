"""Данные для дашборда «Абонентская база»: два графика, мульти-фильтры, API для автообновления."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Callable

from django.conf import settings

from vitrina.clickhouse_client import fetch_rows, qualified_table
from vitrina.client_type_filter import (
    build_client_type_filter_options,
    client_types_where_sql,
    filter_row_key,
)
from vitrina.subscriber_config import get_subscriber_dashboard_config

_LOGGER = logging.getLogger(__name__)

# Полный диапазон дат витрины — для списков фильтров (не зависит от периода на графике).
_CATALOG_DATE_RANGE: tuple[date, date] | None = None

AB_SERIES_KEYS: frozenset[str] = frozenset({"ab0", "ab30", "ab90"})
FLOW_SERIES_KEYS: frozenset[str] = frozenset(
    {
        "inflow_clients",
        "outflow_clients",
        "inflow_agreements",
        "outflow_agreements",
        "inflow_orders",
        "outflow_orders",
    }
)
# По умолчанию на графике притока/оттока — только «Новые клиенты».
FLOW_SERIES_DEFAULT_KEYS: frozenset[str] = frozenset({"inflow_clients"})

_AB_TABLE_BY_KEY: dict[str, str] = {
    "ab0": "kpi_ab0_daily",
    "ab30": "kpi_ab30_daily",
    "ab90": "kpi_ab90_daily",
}

_FLOW_METRIC_BY_KEY: dict[str, tuple[str, str]] = {
    "inflow_clients": ("kpi_inflow_daily", "new_clients"),
    "outflow_clients": ("kpi_outflow_daily", "churned_clients"),
    "inflow_agreements": ("kpi_inflow_daily", "new_agreements"),
    "outflow_agreements": ("kpi_outflow_daily", "completed_agreements"),
    "inflow_orders": ("kpi_inflow_daily", "new_orders"),
    "outflow_orders": ("kpi_outflow_daily", "completed_orders"),
}

_PALETTE_AB = (
    "rgb(59, 130, 246)",
    "rgb(34, 197, 94)",
    "rgb(251, 191, 36)",
)
_PALETTE_FLOW = (
    "rgb(59, 130, 246)",
    "rgb(244, 114, 182)",
    "rgb(34, 197, 94)",
    "rgb(251, 146, 60)",
    "rgb(167, 139, 250)",
    "rgb(248, 113, 113)",
)


@dataclass
class SubscriberBaseFilters:
    date_from: date
    date_to: date
    segment_ids: list[int] = field(default_factory=list)
    service_kinds: list[str] = field(default_factory=list)
    tariff_ids: list[int] = field(default_factory=list)
    client_types: list[str] = field(default_factory=list)


def parse_subscriber_base_filters(
    params: dict[str, Any],
    *,
    date_from_default: date,
    date_to_default: date,
) -> SubscriberBaseFilters:
    df_raw = str(params.get("df") or "").strip()
    dt_raw = str(params.get("dt") or "").strip()
    try:
        df = date.fromisoformat(df_raw) if df_raw else date_from_default
    except ValueError:
        df = date_from_default
    try:
        dt = date.fromisoformat(dt_raw) if dt_raw else date_to_default
    except ValueError:
        dt = date_to_default
    if df > dt:
        df, dt = dt, df

    getlist = getattr(params, "getlist", None)
    if callable(getlist):
        seg_raw = getlist("segment_id")
        sk_raw = getlist("service_kind")
        tid_raw = getlist("tariff_id")
        ct_raw = getlist("client_type")
    else:
        seg_raw = params.get("segment_id") or []
        sk_raw = params.get("service_kind") or []
        tid_raw = params.get("tariff_id") or []
        ct_raw = params.get("client_type") or []
        if isinstance(seg_raw, str):
            seg_raw = [seg_raw]
        if isinstance(sk_raw, str):
            sk_raw = [sk_raw]
        if isinstance(tid_raw, str):
            tid_raw = [tid_raw]
        if isinstance(ct_raw, str):
            ct_raw = [ct_raw]

    segment_ids: list[int] = []
    for s in seg_raw:
        s = str(s).strip()
        if not s:
            continue
        try:
            segment_ids.append(int(s))
        except ValueError:
            continue

    service_kinds = [str(x).strip() for x in sk_raw if str(x).strip()]

    tariff_ids: list[int] = []
    for s in tid_raw:
        s = str(s).strip()
        if not s:
            continue
        try:
            tariff_ids.append(int(s))
        except ValueError:
            continue

    client_types: list[str] = []
    seen_ct: set[str] = set()
    for s in ct_raw:
        v = str(s).strip()
        if not v or v in seen_ct:
            continue
        seen_ct.add(v)
        client_types.append(v)

    return SubscriberBaseFilters(
        date_from=df,
        date_to=dt,
        segment_ids=segment_ids,
        service_kinds=service_kinds,
        tariff_ids=tariff_ids,
        client_types=client_types,
    )


def _fetch_client_type_pairs(f: SubscriberBaseFilters) -> list[tuple[str, str]]:
    """(client_type code, client_type_title) с ненулевой AB0 за период."""
    if not _ch_table_has_column("kpi_ab0_daily", "client_type"):
        return []
    has_title = _ch_table_has_column("kpi_ab0_daily", "client_type_title")
    wh = _where_dims_sql(
        f,
        apply_client_type=False,
        skip_client_types=True,
    )
    label_expr = (
        "coalesce(nullIf(trim(`client_type_title`), ''), `client_type`)"
        if has_title
        else "`client_type`"
    )
    group_sql = "`client_type`" + (", `client_type_title`" if has_title else "")
    sql = (
        f"SELECT `client_type` AS code, {label_expr} AS lbl "
        f"FROM {_fq('kpi_ab0_daily')} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}}{wh} "
        f"AND `client_type` IS NOT NULL AND trim(`client_type`) != '' "
        f"GROUP BY {group_sql} "
        f"HAVING sum(`active_subscribers`) != 0 "
        f"ORDER BY lbl "
        f"LIMIT 500"
    )
    try:
        _cols, rows = fetch_rows(sql, parameters={"df": f.date_from, "dt": f.date_to})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("client_type pairs")
        return []
    out: list[tuple[str, str]] = []
    for row in rows:
        if not row or row[0] is None:
            continue
        code = str(row[0]).strip()
        lbl = str(row[1]).strip() if len(row) > 1 and row[1] is not None else code
        if code:
            out.append((code, lbl or code))
    return out


def fetch_client_type_choices(f: SubscriberBaseFilters) -> list[tuple[str, str]]:
    """(ключ фильтра, подпись) — ключ = client_type_title или код."""
    rows = _fetch_client_type_pairs(f)
    by_key: dict[str, str] = {}
    for code, lbl in rows:
        key = filter_row_key(code, lbl)
        if key:
            by_key[key] = lbl or key
    for s in f.client_types:
        if s not in by_key:
            by_key[s] = s
    return sorted(by_key.items(), key=lambda x: x[1].casefold())


def fetch_filter_options(f: SubscriberBaseFilters) -> dict[str, Any]:
    ct_rows = _fetch_client_type_pairs(f)
    ct_opts = build_client_type_filter_options(ct_rows, selected=list(f.client_types))
    return {
        **ct_opts,
        "segments": [{"value": v, "label": lbl} for v, lbl in fetch_distinct_segments(f)],
        "service_kinds": [{"value": v, "label": lbl} for v, lbl in fetch_distinct_service_kinds(f)],
        "tariffs": [{"value": v, "label": lbl} for v, lbl in fetch_tariff_options_for_filters(f)],
    }


def _fq(table: str) -> str:
    return qualified_table(settings.CLICKHOUSE_DATABASE, table)


def _fetch_catalog_date_range() -> tuple[date, date]:
    """Min/max report_date в kpi_ab0_daily — для eligible-списков фильтров."""
    global _CATALOG_DATE_RANGE
    if _CATALOG_DATE_RANGE is not None:
        return _CATALOG_DATE_RANGE
    sql = f"SELECT min(`report_date`) AS mn, max(`report_date`) AS mx FROM {_fq('kpi_ab0_daily')}"
    try:
        _cols, rows = fetch_rows(sql, parameters={})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("catalog date range")
        today = date.today()
        _CATALOG_DATE_RANGE = (today, today)
        return _CATALOG_DATE_RANGE
    if not rows or rows[0][0] is None or rows[0][1] is None:
        today = date.today()
        _CATALOG_DATE_RANGE = (today, today)
        return _CATALOG_DATE_RANGE
    mn = _normalize_report_date(rows[0][0])
    mx = _normalize_report_date(rows[0][1])
    if mn is None or mx is None:
        today = date.today()
        _CATALOG_DATE_RANGE = (today, today)
        return _CATALOG_DATE_RANGE
    if mn > mx:
        mn, mx = mx, mn
    _CATALOG_DATE_RANGE = (mn, mx)
    return _CATALOG_DATE_RANGE


def _ch_table_exists(table: str) -> bool:
    try:
        sql = (
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tb:String}"
        )
        _cols, rows = fetch_rows(
            sql,
            parameters={"db": settings.CLICKHOUSE_DATABASE, "tb": table},
        )
        return bool(rows) and int(rows[0][0]) > 0
    except Exception:  # noqa: BLE001
        _LOGGER.exception("system.tables check %s", table)
        return False


def _ch_table_has_column(table: str, column: str) -> bool:
    if not _ch_table_exists(table):
        return False
    try:
        sql = (
            "SELECT count() FROM system.columns "
            "WHERE database = {db:String} AND table = {tb:String} AND name = {col:String}"
        )
        _cols, rows = fetch_rows(
            sql,
            parameters={
                "db": settings.CLICKHOUSE_DATABASE,
                "tb": table,
                "col": column,
            },
        )
        if not rows:
            return False
        return int(rows[0][0]) > 0
    except Exception:  # noqa: BLE001
        _LOGGER.exception("system.columns check")
        return False


def _escape_ch_str(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def _where_dims_sql(
    f: SubscriberBaseFilters,
    *,
    apply_client_type: bool,
    table: str = "kpi_ab0_daily",
    skip_segment_ids: bool = False,
    skip_service_kinds: bool = False,
    skip_tariff_ids: bool = False,
    skip_client_types: bool = False,
) -> str:
    parts: list[str] = []
    if not skip_segment_ids and f.segment_ids:
        inner = ",".join(str(int(i)) for i in f.segment_ids)
        parts.append(f" AND `segment_id` IN ({inner}) ")
    if not skip_service_kinds and f.service_kinds:
        lit = ",".join(f"'{_escape_ch_str(x)}'" for x in f.service_kinds)
        parts.append(f" AND `service_kind` IN ({lit}) ")
    if not skip_tariff_ids and f.tariff_ids:
        inner = ",".join(str(int(i)) for i in f.tariff_ids)
        parts.append(f" AND `tariff_id` IN ({inner}) ")
    if apply_client_type and not skip_client_types and f.client_types:
        parts.append(
            client_types_where_sql(
                table,
                list(f.client_types),
                has_client_type=_ch_table_has_column(table, "client_type"),
                has_client_type_title=_ch_table_has_column(table, "client_type_title"),
                escape_str=_escape_ch_str,
            )
        )
    return "".join(parts)


def _chart_metric_pairs() -> tuple[tuple[str, str], ...]:
    """Все (таблица, колонка метрики), участвующие в рядах графиков дашборда."""
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for tbl in _AB_TABLE_BY_KEY.values():
        t = (tbl, "active_subscribers")
        if t not in seen:
            seen.add(t)
            out.append(t)
    for _k, (tbl, col) in _FLOW_METRIC_BY_KEY.items():
        t = (tbl, col)
        if t not in seen:
            seen.add(t)
            out.append(t)
    return tuple(out)


def _dim_nonempty_predicate_sql(dim: str) -> str:
    if dim == "segment_id":
        return " AND `segment_id` IS NOT NULL "
    if dim == "tariff_id":
        return " AND `tariff_id` IS NOT NULL "
    if dim == "service_kind":
        return " AND `service_kind` IS NOT NULL AND trim(`service_kind`) != '' "
    if dim == "client_type":
        return " AND `client_type` IS NOT NULL AND trim(`client_type`) != '' "
    return ""


def _eligible_dimension_strings(
    dim: str,
    f: SubscriberBaseFilters,
    *,
    apply_client_type: bool,
    skip_segment_ids: bool = False,
    skip_service_kinds: bool = False,
    skip_tariff_ids: bool = False,
    skip_client_types: bool = False,
    limit: int = 5000,
    eligible_date_from: date | None = None,
    eligible_date_to: date | None = None,
) -> list[str]:
    """Значения измерения: хотя бы в один день окна сумма по любой метрике графика ≠ 0."""
    wh = _where_dims_sql(
        f,
        apply_client_type=apply_client_type,
        skip_segment_ids=skip_segment_ids,
        skip_service_kinds=skip_service_kinds,
        skip_tariff_ids=skip_tariff_ids,
        skip_client_types=skip_client_types,
    )
    pred = _dim_nonempty_predicate_sql(dim)
    branches: list[str] = []
    for table, col in _chart_metric_pairs():
        if dim == "client_type" and not _ch_table_has_column(table, "client_type"):
            continue
        branches.append(
            f"SELECT toString(`{dim}`) AS _v FROM ("
            f"SELECT `{dim}`, sum(`{col}`) AS _y FROM {_fq(table)} "
            f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}}{wh}{pred} "
            f"GROUP BY `{dim}`, `report_date` "
            f"HAVING coalesce(_y, 0) != 0"
            f")"
        )
    if not branches:
        return []
    df_use = eligible_date_from if eligible_date_from is not None else f.date_from
    dt_use = eligible_date_to if eligible_date_to is not None else f.date_to
    inner = " UNION ALL ".join(branches)
    sql = f"SELECT DISTINCT _v FROM ({inner}) ORDER BY _v LIMIT {int(limit)}"
    try:
        _c, rows = fetch_rows(sql, parameters={"df": df_use, "dt": dt_use})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("eligible dimension %s", dim)
        return []
    out: list[str] = []
    for row in rows:
        if not row or row[0] is None:
            continue
        s = str(row[0]).strip()
        if s:
            out.append(s)
    return out


def _merge_selected_into_label_pairs(
    rows: list[tuple[str, str]],
    selected: list[str],
    *,
    fallback_label: Callable[[str], str] | None = None,
) -> list[tuple[str, str]]:
    by_key: dict[str, str] = dict(rows)
    for s in selected:
        if s not in by_key:
            by_key[s] = fallback_label(s) if fallback_label else s
    return sorted(by_key.items(), key=lambda x: x[0])


def _tariff_fallback_label(tid: str) -> str:
    try:
        ik = int(tid)
    except ValueError:
        return tid
    return f"Тариф {ik}"


def _normalize_report_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if hasattr(val, "date"):
        try:
            return val.date()  # type: ignore[no-any-return]
        except Exception:
            pass
    try:
        return date.fromisoformat(str(val).split(" ")[0])
    except ValueError:
        return None


def _to_chart_number(val: Any) -> float | None:
    if val is None:
        return None
    try:
        n = float(val)
    except (TypeError, ValueError):
        return None
    if n != n:  # NaN
        return None
    return n


def _fetch_daily_sum(
    table: str,
    value_col: str,
    f: SubscriberBaseFilters,
    *,
    apply_client_type: bool,
) -> dict[date, float]:
    if not _ch_table_exists(table):
        return {}
    wh = _where_dims_sql(f, apply_client_type=apply_client_type, table=table)
    sql = (
        f"SELECT `report_date` AS d, sum(`{value_col}`) AS y FROM {_fq(table)} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}} {wh} "
        "GROUP BY `report_date` ORDER BY d"
    )
    try:
        _cols, rows = fetch_rows(sql, parameters={"df": f.date_from, "dt": f.date_to})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("daily sum %s.%s", table, value_col)
        return {}
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


def _daterange(d0: date, d1: date) -> list[date]:
    out: list[date] = []
    d = d0
    while d <= d1:
        out.append(d)
        d += timedelta(days=1)
    return out


def _catalog_eligible_dates() -> tuple[date, date]:
    return _fetch_catalog_date_range()


def fetch_distinct_segments(f: SubscriberBaseFilters) -> list[tuple[str, str]]:
    has_ct = _ch_table_has_column("kpi_ab0_daily", "client_type")
    apply_ct = has_ct and bool(f.client_types)
    cat_df, cat_dt = _catalog_eligible_dates()
    eligible = _eligible_dimension_strings(
        "segment_id",
        f,
        apply_client_type=apply_ct,
        skip_segment_ids=True,
        limit=500,
        eligible_date_from=cat_df,
        eligible_date_to=cat_dt,
    )
    from vitrina.segment_labels import apply_segment_labels, segment_display_label

    rows = apply_segment_labels([(x, x) for x in eligible])
    sel = [str(i) for i in f.segment_ids]
    return _merge_selected_into_label_pairs(rows, sel, fallback_label=segment_display_label)


def fetch_distinct_service_kinds(f: SubscriberBaseFilters) -> list[tuple[str, str]]:
    has_ct = _ch_table_has_column("kpi_ab0_daily", "client_type")
    apply_ct = has_ct and bool(f.client_types)
    cat_df, cat_dt = _catalog_eligible_dates()
    eligible = _eligible_dimension_strings(
        "service_kind",
        f,
        apply_client_type=apply_ct,
        skip_service_kinds=True,
        limit=300,
        eligible_date_from=cat_df,
        eligible_date_to=cat_dt,
    )
    rows = [(x, x) for x in eligible]
    return _merge_selected_into_label_pairs(rows, list(f.service_kinds))


def fetch_tariff_options_for_filters(f: SubscriberBaseFilters) -> list[tuple[str, str]]:
    """Тарифы с активностью по метрикам графика за полный каталог дат (+ текущий выбор)."""
    has_ct = _ch_table_has_column("kpi_ab0_daily", "client_type")
    apply_ct = has_ct and bool(f.client_types)
    cat_df, cat_dt = _catalog_eligible_dates()
    eligible = _eligible_dimension_strings(
        "tariff_id",
        f,
        apply_client_type=apply_ct,
        skip_tariff_ids=True,
        limit=800,
        eligible_date_from=cat_df,
        eligible_date_to=cat_dt,
    )
    ids_ordered: list[int] = []
    seen_i: set[int] = set()
    # Сначала выбранные пользователем — чтобы не потерять их при обрезке IN (...) до 800 id.
    for tid in f.tariff_ids:
        if tid not in seen_i:
            seen_i.add(tid)
            ids_ordered.append(tid)
    for s in eligible:
        try:
            ik = int(s)
        except ValueError:
            continue
        if ik not in seen_i:
            seen_i.add(ik)
            ids_ordered.append(ik)
    if not ids_ordered:
        return []

    seg_wh = ""
    if f.segment_ids:
        inner = ",".join(str(int(i)) for i in f.segment_ids)
        seg_wh = f" AND `segment_id` IN ({inner}) "
    sk_wh = ""
    if f.service_kinds:
        lit = ",".join(f"'{_escape_ch_str(x)}'" for x in f.service_kinds)
        sk_wh = f" AND `service_kind` IN ({lit}) "
    ct_wh = ""
    if apply_ct and f.client_types:
        ct_wh = client_types_where_sql(
            "kpi_ab0_daily",
            list(f.client_types),
            has_client_type=True,
            has_client_type_title=_ch_table_has_column("kpi_ab0_daily", "client_type_title"),
            escape_str=_escape_ch_str,
        )

    inner_ids = ",".join(str(i) for i in ids_ordered[:800])
    sql = (
        f"SELECT `tariff_id`, anyLast(`tariff_title`) AS tt FROM {_fq('kpi_ab0_daily')} "
        f"WHERE `report_date` BETWEEN {{df:Date}} AND {{dt:Date}} "
        f"AND `tariff_id` IS NOT NULL AND `tariff_id` IN ({inner_ids}) "
        f"{seg_wh}{sk_wh}{ct_wh} "
        "GROUP BY `tariff_id` ORDER BY tt NULLS LAST, tariff_id LIMIT 800"
    )
    try:
        _c, rows = fetch_rows(sql, parameters={"df": cat_df, "dt": cat_dt})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("tariff options")
        return []
    out: list[tuple[str, str]] = []
    for r in rows:
        if not r or r[0] is None:
            continue
        tid = int(r[0])
        tt = (str(r[1]).strip() if r[1] is not None else "") or f"Тариф {tid}"
        out.append((str(tid), tt))
    out.sort(key=lambda x: x[1].lower())
    known = {x[0] for x in out}
    for tid in f.tariff_ids:
        ts = str(int(tid))
        if ts not in known:
            out.append((ts, _tariff_fallback_label(ts)))
            known.add(ts)
    out.sort(key=lambda x: x[1].lower())
    return out


def _chart_point_value(series_data: dict[date, float], d: date) -> float:
    """Нет значения за день — на графике 0."""
    v = _to_chart_number(series_data.get(d))
    return 0.0 if v is None else v


def _dataset_entry(
    *,
    key: str,
    label: str,
    series_data: dict[date, float],
    labels_d: list[date],
    color: str,
) -> dict[str, Any]:
    data = [_chart_point_value(series_data, d) for d in labels_d]
    return {
        "key": key,
        "label": label,
        "data": data,
        "borderColor": color,
        "backgroundColor": color.replace("rgb", "rgba").replace(")", ", 0.12)"),
        "borderWidth": 2,
        "fill": False,
        "tension": 0.15,
        "spanGaps": False,
        "pointRadius": 3,
        "pointHoverRadius": 6,
        "pointHitRadius": 8,
        "pointBackgroundColor": color,
        "pointBorderColor": color,
        "pointBorderWidth": 1,
    }


def _build_chart_block(
    f: SubscriberBaseFilters,
    *,
    chart_cfg: dict[str, Any],
    series_specs: list[tuple[str, str, str]],
    visible_keys: set[str],
    palette: tuple[str, ...],
    apply_ct: bool,
) -> dict[str, Any]:
    series_labels = chart_cfg.get("series") or {}
    metrics: dict[str, dict[date, float]] = {}

    for key, table, col in series_specs:
        if key not in visible_keys:
            continue
        metrics[key] = _fetch_daily_sum(table, col, f, apply_client_type=apply_ct)

    labels_d = _daterange(f.date_from, f.date_to)
    labels = [d.isoformat() for d in labels_d]

    datasets: list[dict[str, Any]] = []
    for i, (key, table, col) in enumerate(series_specs):
        if key not in visible_keys:
            continue
        lbl = str(series_labels.get(key, key))
        color = palette[i % len(palette)]
        datasets.append(
            _dataset_entry(
                key=key,
                label=lbl,
                series_data=metrics[key],
                labels_d=labels_d,
                color=color,
            )
        )

    return {
        "title": chart_cfg.get("title", ""),
        "labels": labels,
        "datasets": datasets,
        "xlabel": chart_cfg.get("xlabel", "Дата"),
        "ylabel": chart_cfg.get("ylabel", ""),
        "series_legend": chart_cfg.get("series_legend", ""),
        "series_keys": [k for k, _t, _c in series_specs],
    }


def _prune_client_types(f: SubscriberBaseFilters) -> SubscriberBaseFilters:
    if not f.client_types:
        return f
    valid = {code for code, _ in fetch_client_type_choices(f)}
    if not valid:
        return f
    kept = [c for c in f.client_types if c in valid]
    if kept == f.client_types:
        return f
    return SubscriberBaseFilters(
        date_from=f.date_from,
        date_to=f.date_to,
        segment_ids=list(f.segment_ids),
        service_kinds=list(f.service_kinds),
        tariff_ids=list(f.tariff_ids),
        client_types=kept,
    )


def _empty_chart_block(chart_cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": chart_cfg.get("title", ""),
        "labels": [],
        "datasets": [],
        "xlabel": chart_cfg.get("xlabel", "Дата"),
        "ylabel": chart_cfg.get("ylabel", ""),
        "series_legend": chart_cfg.get("series_legend", ""),
        "series_keys": [],
    }


def build_subscriber_api_payload(
    f: SubscriberBaseFilters,
    params: dict[str, Any],
) -> dict[str, Any]:
    cfg = get_subscriber_dashboard_config()
    has_ct = _ch_table_has_column("kpi_ab0_daily", "client_type")
    f = _prune_client_types(f)
    apply_ct = has_ct and bool(f.client_types)
    filters = fetch_filter_options(f)

    ab_specs = [(k, _AB_TABLE_BY_KEY[k], "active_subscribers") for k in ("ab0", "ab30", "ab90")]
    flow_specs = [(k, _FLOW_METRIC_BY_KEY[k][0], _FLOW_METRIC_BY_KEY[k][1]) for k in _FLOW_METRIC_BY_KEY]

    charts_cfg = cfg.get("charts") or {}
    try:
        ab_chart = _build_chart_block(
            f,
            chart_cfg=charts_cfg.get("ab") or {},
            series_specs=ab_specs,
            visible_keys=set(AB_SERIES_KEYS),
            palette=_PALETTE_AB,
            apply_ct=apply_ct,
        )
    except Exception:  # noqa: BLE001
        _LOGGER.exception("subscriber ab chart")
        ab_chart = _empty_chart_block(charts_cfg.get("ab") or {})
    try:
        flow_chart = _build_chart_block(
            f,
            chart_cfg=charts_cfg.get("flow") or {},
            series_specs=flow_specs,
            visible_keys=set(FLOW_SERIES_KEYS),
            palette=_PALETTE_FLOW,
            apply_ct=apply_ct,
        )
    except Exception:  # noqa: BLE001
        _LOGGER.exception("subscriber flow chart")
        flow_chart = _empty_chart_block(charts_cfg.get("flow") or {})

    return {
        "ok": True,
        "date_from": f.date_from.isoformat(),
        "date_to": f.date_to.isoformat(),
        "has_client_type_column": has_ct,
        "charts": {"ab": ab_chart, "flow": flow_chart},
        "filters": filters,
        "selected": {
            "segment_ids": [str(i) for i in f.segment_ids],
            "service_kinds": list(f.service_kinds),
            "tariff_ids": [str(i) for i in f.tariff_ids],
            "client_types": list(f.client_types),
        },
        "config": cfg,
    }
