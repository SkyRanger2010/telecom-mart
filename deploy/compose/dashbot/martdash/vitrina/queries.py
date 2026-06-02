"""Параметризованные KPI и drilldown-выборки по витринам ClickHouse."""

from __future__ import annotations

from datetime import date
from typing import Any

from django.conf import settings

from vitrina.clickhouse_client import fetch_rows, qualified_table
from vitrina.metadata import MeasureRatio, MeasureSum, VitrinaDef

DIM_TARIFF_TABLE = "dim_tariff"

DATE_GROUP = "__date__"

DIM_LABEL_RU: dict[str, str] = {
    "segment_id": "Сегмент",
    "tariff_id": "Тариф",
    "tariff_title": "Название тарифа",
    "service_kind": "Тип услуги",
    "business_center_id": "Центр ответственности",
}


def _cols_allowed(v: VitrinaDef) -> set[str]:
    s = {v.date_column, *v.dimension_cols}
    for m in v.measures:
        if isinstance(m, MeasureSum):
            s.add(m.column)
        elif isinstance(m, MeasureRatio):
            s.add(m.numerator_col)
            s.add(m.denominator_col)
    return s


def _qi(ident: str, allowed: set[str]) -> str:
    if ident not in allowed:
        raise ValueError(f"Столбец не из описания витрины: {ident}")
    return "`" + ident.replace("`", "``") + "`"


def _measure_fragments(v: VitrinaDef) -> tuple[list[str], list[tuple[str, str]]]:
    parts: list[str] = []
    labels: list[tuple[str, str]] = []
    allowed = _cols_allowed(v)
    for idx, m in enumerate(v.measures):
        alias = f"k_{idx}"
        if isinstance(m, MeasureSum):
            col_sql = _qi(m.column, allowed)
            match m.agg:
                case "sum":
                    frag = f"sum({col_sql}) AS `{alias}`"
                case "avg":
                    frag = f"avg({col_sql}) AS `{alias}`"
                case _:
                    raise ValueError(f"неизвестный agg: {m.agg}")
        elif isinstance(m, MeasureRatio):
            num_sql = _qi(m.numerator_col, allowed)
            den_sql = _qi(m.denominator_col, allowed)
            ratio_expr = f"sum({num_sql}) / nullIf(sum({den_sql}), 0)"
            if m.round_decimals is not None:
                ratio_expr = f"round({ratio_expr}, {int(m.round_decimals)})"
            frag = f"{ratio_expr} AS `{alias}`"
        else:
            raise TypeError(type(m))
        parts.append(frag)
        labels.append((alias, m.label_ru))
    return parts, labels


def _dimension_filters_sql(v: VitrinaDef, filters: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    allowed = _cols_allowed(v)
    exprs: list[str] = []
    params: dict[str, Any] = {}
    p = 0
    for col, val in filters.items():
        if val is None or val == "":
            continue
        if col == v.date_column:
            continue
        if col not in v.dimension_cols:
            continue
        qc = _qi(col, allowed)
        if col == "service_kind" or col == "tariff_title":
            key = f"sk{p}"
            params[key] = str(val)
            exprs.append(f"{qc} = {{{key}:String}}")
        else:
            key = f"di{p}"
            params[key] = int(val)
            exprs.append(f"{qc} = {{{key}:Int64}}")
        p += 1
    return exprs, params


def fetch_tariff_id_to_title() -> dict[int, str]:
    """Справочник тарифов в serving (таблица dim_tariff). Пустой словарь при отсутствии таблицы или ошибке."""
    fq = qualified_table(settings.CLICKHOUSE_DATABASE, DIM_TARIFF_TABLE)
    sql = f"SELECT `tariff_id`, `tariff_title` FROM {fq}"
    try:
        _cols, rows = fetch_rows(sql)
    except Exception:
        return {}
    out: dict[int, str] = {}
    for row in rows:
        if len(row) < 2:
            continue
        tid, title = row[0], row[1]
        if tid is None:
            continue
        try:
            ik = int(tid)
        except (TypeError, ValueError):
            continue
        label = (str(title).strip() if title is not None else "") or f"Тариф {ik}"
        out[ik] = label
    return out


def fetch_date_range(
    v: VitrinaDef,
    *,
    filters: dict[str, Any],
) -> tuple[date | None, date | None]:
    """Min/max ключа даты по таблице с учётом только разрезов (без фильтра по дате)."""
    fq = qualified_table(settings.CLICKHOUSE_DATABASE, v.table)
    allowed = _cols_allowed(v)
    dc = _qi(v.date_column, allowed)
    filt_sql, filt_params = _dimension_filters_sql(v, filters)
    if filt_sql:
        ws = " AND ".join(filt_sql)
        sql = f"SELECT min({dc}) AS mn, max({dc}) AS mx FROM {fq} WHERE {ws}"
    else:
        sql = f"SELECT min({dc}) AS mn, max({dc}) AS mx FROM {fq}"
    _cols, rows = fetch_rows(sql, parameters=filt_params)
    if not rows:
        return None, None
    mn, mx = rows[0][0], rows[0][1]
    if mn is None and mx is None:
        return None, None
    if hasattr(mn, "date"):
        mn = mn.date()  # type: ignore[assignment]
    if hasattr(mx, "date"):
        mx = mx.date()  # type: ignore[assignment]
    if isinstance(mn, str):
        mn = date.fromisoformat(mn.split(" ")[0])
    if isinstance(mx, str):
        mx = date.fromisoformat(mx.split(" ")[0])
    if not isinstance(mn, date) or not isinstance(mx, date):
        return None, None
    return mn, mx


def _base_where_sql(
    v: VitrinaDef,
    *,
    date_from: date,
    date_to: date,
    filters: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    allowed = _cols_allowed(v)
    dc = _qi(v.date_column, allowed)
    parts = [f"{dc} BETWEEN {{df:Date}} AND {{dt:Date}}"]
    params: dict[str, Any] = {"df": date_from, "dt": date_to}
    filt_sql, filt_params = _dimension_filters_sql(v, filters)
    parts.extend(filt_sql)
    params.update(filt_params)
    return parts, params


def fetch_distinct_dimension_values(
    v: VitrinaDef,
    *,
    date_from: date,
    date_to: date,
    filters: dict[str, Any],
    dim: str,
    limit: int = 500,
) -> list[Any]:
    """Уникальные значения измерения за период; прочие разрезы из ``filters`` без ключа ``dim``."""
    if dim not in v.dimension_cols:
        return []
    lim = min(max(int(limit), 1), 5000)
    allowed = _cols_allowed(v)
    qc = _qi(dim, allowed)
    filt_other = {k: val for k, val in filters.items() if k != dim}
    wc, wp = _base_where_sql(v, date_from=date_from, date_to=date_to, filters=filt_other)
    ws = " AND ".join(wc)
    fq = qualified_table(settings.CLICKHOUSE_DATABASE, v.table)
    sql = (
        f"SELECT DISTINCT {qc} AS d FROM {fq} "
        f"WHERE {ws} AND {qc} IS NOT NULL "
        f"ORDER BY d LIMIT {lim}"
    )
    _cols, rows = fetch_rows(sql, parameters=wp)
    out: list[Any] = []
    for row in rows:
        if row and row[0] is not None:
            out.append(row[0])
    return out


def fetch_kpi_cards(
    v: VitrinaDef,
    *,
    date_from: date,
    date_to: date,
    filters: dict[str, Any],
) -> list[tuple[str, Any]]:
    """Пары «подпись KPI — значение»."""
    frags, lbls = _measure_fragments(v)
    fq = qualified_table(settings.CLICKHOUSE_DATABASE, v.table)
    wc, wp = _base_where_sql(v, date_from=date_from, date_to=date_to, filters=filters)
    ws = " AND ".join(wc)
    sql = f"SELECT {', '.join(frags)} FROM {fq} WHERE {ws}"
    cols, rows = fetch_rows(sql, parameters=wp)
    row0 = rows[0] if rows else tuple(None for _ in cols)
    out: list[tuple[str, Any]] = []
    for i, (alias, label) in enumerate(lbls):
        val = row0[i] if i < len(row0) else None
        out.append((label, val))
    return out


def fetch_breakdown(
    v: VitrinaDef,
    *,
    date_from: date,
    date_to: date,
    filters: dict[str, Any],
    group_axis: str,
    row_limit: int | None = None,
) -> tuple[str, list[str], list[list[Any]]]:
    """
    Drilldown по дате (DATE_GROUP) или по измерению витрины.
    Первая колонка — значение оси; при группировке по ``tariff_id`` и наличии колонки
    ``tariff_title`` во витрине добавляется вторая колонка (``max(tariff_title)``), затем агрегаты KPI.
    """
    if row_limit is None:
        row_limit = 15000 if group_axis == DATE_GROUP else 400
    if row_limit <= 0 or row_limit > 50000:
        row_limit = 15000 if group_axis == DATE_GROUP else 400
    fq = qualified_table(settings.CLICKHOUSE_DATABASE, v.table)
    allowed = _cols_allowed(v)
    frags, lbls = _measure_fragments(v)
    wc, wp = _base_where_sql(v, date_from=date_from, date_to=date_to, filters=filters)

    if group_axis == DATE_GROUP:
        grp = _qi(v.date_column, allowed)
        axis_sql = f"{grp} AS `axis_breakdown`"
        group_exprs = grp
        order_exprs = grp
        axis_header_ru = f"Дата ({v.date_column})"
        title_agg_sql = ""
    else:
        if group_axis not in v.dimension_cols:
            raise ValueError("недопустимая группировка")
        grp = _qi(group_axis, allowed)
        axis_sql = f"{grp} AS `axis_breakdown`"
        axis_header_ru = DIM_LABEL_RU.get(group_axis, group_axis)
        group_exprs = grp
        order_exprs = grp
        title_agg_sql = ""
        if group_axis == "tariff_id" and "tariff_title" in allowed:
            tt = _qi("tariff_title", allowed)
            title_agg_sql = f", max({tt}) AS `axis_tariff_title`"

    ws = " AND ".join(wc)
    order_dir = "ASC" if group_axis == DATE_GROUP else "DESC"
    measure_sql = f", {', '.join(frags)}" if frags else ""
    sql = (
        f"SELECT {axis_sql}{title_agg_sql}{measure_sql} FROM {fq} "
        f"WHERE {ws} GROUP BY {group_exprs} ORDER BY {order_exprs} {order_dir} LIMIT {row_limit}"
    )

    _colnames, rows = fetch_rows(sql, parameters=wp)
    extra_headers: list[str] = []
    if group_axis == "tariff_id" and "tariff_title" in allowed:
        extra_headers.append(DIM_LABEL_RU["tariff_title"])
    headers_ru = [axis_header_ru, *extra_headers, *[t[1] for t in lbls]]
    return sql, headers_ru, [list(row) for row in rows]


def grouping_choices_vitrina(v: VitrinaDef) -> list[tuple[str, str]]:
    """Значение для формы группировки и подпись."""
    opts: list[tuple[str, str]] = [(DATE_GROUP, "Календарь (ключ даты)")]
    for d in v.dimension_cols:
        lbl = DIM_LABEL_RU.get(d, d)
        opts.append((d, lbl))
    return opts


def parse_dimension_filters(GET: dict[str, Any], v: VitrinaDef) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for d in v.dimension_cols:
        raw = GET.get(f"f_{d}", "").strip()
        if raw == "":
            continue
        if d == "service_kind" or d == "tariff_title":
            parsed[d] = raw
        else:
            parsed[d] = int(raw)
    return parsed


def parse_iso_date_optional(s: str | None, default: date) -> date:
    if not s or not str(s).strip():
        return default
    return date.fromisoformat(str(s).strip())
