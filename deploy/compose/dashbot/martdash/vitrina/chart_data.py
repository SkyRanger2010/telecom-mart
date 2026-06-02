"""Данные для Chart.js (временной ряд или гистограмма по разрезу)."""

from __future__ import annotations

from datetime import date
from typing import Any

from vitrina.queries import DATE_GROUP, DIM_LABEL_RU

_PALETTE = (
    "rgb(59, 130, 246)",
    "rgb(34, 197, 94)",
    "rgb(251, 191, 36)",
    "rgb(244, 114, 182)",
    "rgb(167, 139, 250)",
)


def _to_chart_number(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_axis_as_date(val: Any) -> date | None:
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


def _date_label(val: Any) -> str:
    d = _parse_axis_as_date(val)
    return d.isoformat() if d else "?"


def build_ab0_line_chart(headers: list[str], rows: list[list[Any]]) -> dict[str, Any] | None:
    """Одна линия: дата (ось X) → сумма активных абонентов за день (ось Y)."""
    if not rows or len(headers) < 2:
        return None

    keyed: list[tuple[date | None, list[Any]]] = []
    for r in rows:
        d = _parse_axis_as_date(r[0] if r else None)
        keyed.append((d, list(r)))

    def sort_key(item: tuple[date | None, list[Any]]) -> tuple[int, date]:
        d, _ = item
        if d is None:
            return (1, date.min)
        return (0, d)

    keyed.sort(key=sort_key)
    labels = [_date_label(d) for d, _ in keyed]
    col = [_to_chart_number(r[1]) if len(r) > 1 else None for _, r in keyed]
    measure_label = headers[1] if len(headers) > 1 else "Активные абоненты"
    c = _PALETTE[0]

    return {
        "kind": "line",
        "title": "Активные абоненты (AB0) по дням",
        "xlabel": "Дата",
        "ylabel": "Активные абоненты",
        "labels": labels,
        "datasets": [
            {
                "label": measure_label,
                "data": col,
                "borderColor": c,
                "backgroundColor": c.replace("rgb", "rgba").replace(")", ", 0.12)"),
                "fill": False,
                "tension": 0.2,
                "borderWidth": 2,
                "pointRadius": 4,
                "pointHoverRadius": 7,
                "pointHitRadius": 8,
                "pointBackgroundColor": c,
                "pointBorderColor": c,
                "pointBorderWidth": 1,
            }
        ],
    }


def build_chart_payload(
    *,
    group_axis: str,
    headers: list[str],
    rows: list[list[Any]],
) -> dict[str, Any] | None:
    if not rows or len(headers) < 2:
        return None

    if group_axis == DATE_GROUP:
        keyed: list[tuple[date | None, list[Any]]] = []
        for r in rows:
            d = _parse_axis_as_date(r[0] if r else None)
            keyed.append((d, list(r)))

        def sort_key(item: tuple[date | None, list[Any]]) -> tuple[int, date]:
            d, _ = item
            if d is None:
                return (1, date.min)
            return (0, d)

        keyed.sort(key=sort_key)
        labels = [_date_label(d) for d, _ in keyed]
        datasets: list[dict[str, Any]] = []
        for j in range(1, len(headers)):
            col = [_to_chart_number(r[j]) if j < len(r) else None for _, r in keyed]
            c = _PALETTE[(j - 1) % len(_PALETTE)]
            datasets.append(
                {
                    "label": headers[j],
                    "data": col,
                    "borderColor": c,
                    "backgroundColor": c.replace("rgb", "rgba").replace(")", ", 0.15)"),
                    "fill": False,
                    "tension": 0.15,
                    "borderWidth": 2,
                    "pointRadius": 3,
                    "pointHoverRadius": 6,
                    "pointHitRadius": 8,
                    "pointBackgroundColor": c,
                    "pointBorderColor": c.replace("rgb", "rgba").replace(")", ", 0.35)"),
                    "pointBorderWidth": 1,
                }
            )
        return {
            "kind": "line",
            "title": "Динамика по датам",
            "xlabel": "Дата",
            "ylabel": "Значение",
            "labels": labels,
            "datasets": datasets,
        }

    ms = 1
    if (
        group_axis == "tariff_id"
        and len(headers) >= 3
        and headers[1] == DIM_LABEL_RU.get("tariff_title")
    ):
        ms = 2

    def _tariff_bar_label(r: list[Any]) -> str:
        if len(r) < 2:
            return "" if r[0] is None else str(r[0])
        tid, title = r[0], r[1]
        t = (str(title).strip() if title is not None else "") or ""
        if t:
            return f"{t} ({tid})" if tid is not None else t
        return "" if tid is None else str(tid)

    if ms == 2:
        labels = [_tariff_bar_label(r) for r in rows]
    else:
        labels = ["" if r[0] is None else str(r[0]) for r in rows]
    datasets = []
    for j in range(ms, len(headers)):
        col = [_to_chart_number(r[j]) if j < len(r) else None for r in rows]
        c = _PALETTE[(j - ms) % len(_PALETTE)]
        datasets.append(
            {
                "label": headers[j],
                "data": col,
                "borderColor": c,
                "backgroundColor": c.replace("rgb", "rgba").replace(")", ", 0.55)"),
                "borderWidth": 1,
            }
        )
    index_axis = "y" if len(rows) > 14 else "x"
    return {
        "kind": "bar",
        "title": headers[0] if headers else "Разрез",
        "labels": labels,
        "datasets": datasets,
        "indexAxis": index_axis,
    }
