"""Автовыбор визуализации по таблице результата NL2SQL (таблица, график, KPI)."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

RESULT_NUMERIC_DECIMALS = 3

_DATE_NAME_HINTS = frozenset(
    {
        "date",
        "dt",
        "day",
        "report_date",
        "period_end",
        "month_start",
        "дата",
        "день",
        "период",
        "месяц",
    }
)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
_CHART_PALETTE = (
    "rgb(59, 130, 246)",
    "rgb(34, 197, 94)",
    "rgb(251, 191, 36)",
    "rgb(244, 114, 182)",
    "rgb(167, 139, 250)",
)

VIZ_LABELS = {
    "table": "Таблица",
    "timeseries": "Временной ряд",
    "kpi": "Плитки KPI",
    "empty": "Нет данных",
}

EMPTY_RESULT_MESSAGE = "По вашему запросу данных не найдено."
EMPTY_RESULT_HINT = (
    "Уточните период или фильтры, проверьте формулировку вопроса и попробуйте переформулировать запрос."
)


def empty_result_fields() -> dict[str, Any]:
    return {
        "empty": True,
        "message": EMPTY_RESULT_MESSAGE,
        "hint": EMPTY_RESULT_HINT,
        "viz_type": "empty",
        "viz_label": VIZ_LABELS["empty"],
        "chart": None,
    }


def rows_as_tuples(rows: list[list[Any]] | list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    return [tuple(r) if isinstance(r, (list, tuple)) else (r,) for r in rows]


def _round_numeric_for_display(val: Any) -> Any:
    """Нецелые числа — до RESULT_NUMERIC_DECIMALS знаков; целые — без дробной части."""
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return val
    num: float | None = None
    if isinstance(val, float):
        num = val
    elif isinstance(val, Decimal):
        num = float(val)
    if num is None:
        return val
    rounded = round(num, RESULT_NUMERIC_DECIMALS)
    if rounded == int(rounded):
        return int(rounded)
    return rounded


def serialize_cell(val: Any) -> Any:
    if val is not None and not isinstance(val, (int, float, bool, Decimal)):
        if hasattr(val, "isoformat"):
            try:
                return val.isoformat()
            except Exception:
                return str(val)
    return _round_numeric_for_display(val)


def serialize_row(row: tuple[Any, ...] | list[Any]) -> tuple[Any, ...]:
    return tuple(serialize_cell(c) for c in row)


def serialize_rows(rows: list[tuple[Any, ...]] | list[list[Any]]) -> list[list[Any]]:
    out: list[list[Any]] = []
    for row in rows:
        if isinstance(row, (list, tuple)):
            out.append(list(serialize_row(row)))
        else:
            out.append([serialize_cell(row)])
    return out


# Обратная совместимость внутренних вызовов
_serialize_cell = serialize_cell


def _date_key(val: Any) -> str:
    if val is None:
        return "?"
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    s = str(_serialize_cell(val))
    if "T" in s:
        s = s.split("T", 1)[0]
    return s.split(" ", 1)[0]


def _parse_as_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if not s:
        return None
    if "T" in s:
        s = s.split("T", 1)[0]
    s = s.split(" ", 1)[0]
    if _ISO_DATE_RE.match(s):
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None
    return None


def _is_numeric_column(rows: list[tuple[Any, ...]], col_idx: int) -> bool:
    for row in rows[:50]:
        if col_idx >= len(row):
            continue
        v = row[col_idx]
        if v is None:
            continue
        if isinstance(v, (int, float)):
            return True
        try:
            float(v)
            return True
        except (TypeError, ValueError):
            return False
    return False


def _column_values_look_like_dates(rows: list[tuple[Any, ...]], col_idx: int) -> bool:
    checked = 0
    parsed = 0
    for row in rows[:40]:
        if col_idx >= len(row):
            continue
        v = row[col_idx]
        if v is None:
            continue
        checked += 1
        if _parse_as_date(v) is not None:
            parsed += 1
    return checked >= 3 and parsed / checked >= 0.7


def _column_name_looks_like_date(name: str) -> bool:
    cl = name.lower().replace("`", "").replace('"', "").strip()
    if "date" in cl:
        return True
    if cl in _DATE_NAME_HINTS:
        return True
    return any(h in cl for h in ("дата", "период", "месяц", "день"))


def find_date_col(columns: list[str], rows: list[tuple[Any, ...]]) -> int | None:
    for i, c in enumerate(columns):
        if _column_name_looks_like_date(c):
            return i
    for i in range(len(columns)):
        if _column_values_look_like_dates(rows, i):
            return i
    return None


def infer_viz_type(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    """table | timeseries | kpi — 1 строка → KPI; дата + метрики → график; иначе таблица."""
    if not columns or not rows:
        return "table"
    nrows = len(rows)
    ncol = len(columns)
    if nrows == 1:
        return "kpi"
    date_i = find_date_col(columns, rows)
    num_cols = [i for i in range(ncol) if i != date_i and _is_numeric_column(rows, i)]

    if date_i is not None and num_cols and nrows >= 2:
        return "timeseries"
    return "table"


def _rgba(rgb: str, alpha: float) -> str:
    return rgb.replace("rgb(", "rgba(").replace(")", f", {alpha})")


def _dataset_key(label: str, index: int) -> str:
    slug = re.sub(r"\W+", "_", str(label), flags=re.UNICODE).strip("_")[:40] or "series"
    return f"s{index}_{slug}"


def build_chart_spec(
    viz_type: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> dict[str, Any] | None:
    if viz_type == "table":
        return None

    date_i = find_date_col(columns, rows)
    num_cols = [i for i in range(len(columns)) if i != date_i and _is_numeric_column(rows, i)]

    if viz_type == "kpi":
        kpis: list[dict[str, str]] = []
        row = rows[0]
        for i, col in enumerate(columns):
            if i >= len(row):
                continue
            if date_i is not None and i == date_i:
                continue
            val = row[i]
            kpis.append({"label": col, "value": "—" if val is None else str(val)})
        if not kpis and columns:
            kpis = [{"label": columns[0], "value": str(row[0]) if row else "—"}]
        return {"kpis": kpis[:12]}

    if viz_type == "timeseries" and date_i is not None and num_cols:
        dim_cols = [i for i in range(len(columns)) if i != date_i and i not in num_cols]
        label_idx = dim_cols[0] if dim_cols else None

        dates_sorted: list[str] = []
        seen_d: set[str] = set()
        # series_key -> {date -> value}
        series_map: dict[str, dict[str, float]] = {}

        metric_cols = num_cols if label_idx is None else num_cols[:1]

        for row in rows:
            if date_i >= len(row):
                continue
            d_key = _date_key(row[date_i])
            if d_key not in seen_d:
                seen_d.add(d_key)
                dates_sorted.append(d_key)

            dim_label = ""
            if label_idx is not None and label_idx < len(row) and row[label_idx] is not None:
                dim_label = str(row[label_idx])

            for y_col in metric_cols:
                series_name = columns[y_col]
                if dim_label:
                    series_name = f"{dim_label} · {series_name}" if len(metric_cols) > 1 else dim_label
                if series_name not in series_map:
                    series_map[series_name] = {}
                yv = row[y_col] if y_col < len(row) else 0
                try:
                    raw = float(yv) if yv is not None else 0.0
                    rounded = _round_numeric_for_display(raw)
                    series_map[series_name][d_key] = (
                        float(rounded) if isinstance(rounded, (int, float)) else 0.0
                    )
                except (TypeError, ValueError):
                    series_map[series_name][d_key] = 0.0

        dates_sorted.sort()
        datasets = []
        for si, (name, by_date) in enumerate(sorted(series_map.items())):
            color = _CHART_PALETTE[si % len(_CHART_PALETTE)]
            datasets.append(
                {
                    "key": _dataset_key(name, si),
                    "label": name,
                    "data": [by_date.get(d, 0) for d in dates_sorted],
                    "borderColor": color,
                    "backgroundColor": color,
                    "fill": False,
                    "tension": 0.15,
                }
            )
        ylabel = ", ".join(columns[i] for i in metric_cols[:3])
        xlabel = columns[date_i] if date_i is not None and date_i < len(columns) else "Дата"
        return {
            "type": "line",
            "labels": dates_sorted,
            "datasets": datasets,
            "xlabel": xlabel,
            "ylabel": ylabel,
        }

    return None


def build_viz_payload(columns: list[str], rows: list[list[Any]] | list[tuple[Any, ...]]) -> dict[str, Any]:
    """viz_type, viz_label, chart — для ответов API."""
    row_tuples = rows_as_tuples(rows)
    viz_type = infer_viz_type(columns, row_tuples)
    return {
        "viz_type": viz_type,
        "viz_label": VIZ_LABELS.get(viz_type, viz_type),
        "chart": build_chart_spec(viz_type, columns, row_tuples),
    }
