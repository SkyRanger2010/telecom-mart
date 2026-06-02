"""Подписи segment_id из serving.dim_segment (иерархия «родитель / … / узел»)."""

from __future__ import annotations

import logging
from functools import lru_cache

from django.conf import settings

from vitrina.clickhouse_client import fetch_rows
from vitrina.subscriber_base_data import _fq

_LOGGER = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _segment_title_by_id_cached() -> dict[str, str]:
    if not _ch_has_dim_segment():
        return {}
    sql = (
        f"SELECT toString(`segment_id`) AS id, "
        f"nullIf(trim(`segment_title`), '') AS title "
        f"FROM {_fq('dim_segment')} "
        "WHERE `segment_id` IS NOT NULL"
    )
    try:
        _cols, rows = fetch_rows(sql, parameters={})
    except Exception:  # noqa: BLE001
        _LOGGER.exception("dim_segment labels")
        return {}
    out: dict[str, str] = {}
    for row in rows:
        if not row or row[0] is None:
            continue
        sid = str(row[0]).strip()
        if not sid:
            continue
        title = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
        out[sid] = title or sid
    return out


def _ch_has_dim_segment() -> bool:
    try:
        sql = (
            "SELECT count() FROM system.tables "
            "WHERE database = {db:String} AND name = {tb:String}"
        )
        _cols, rows = fetch_rows(
            sql,
            parameters={"db": settings.CLICKHOUSE_DATABASE, "tb": "dim_segment"},
        )
        return bool(rows and int(rows[0][0]) > 0)
    except Exception:  # noqa: BLE001
        return False


def segment_display_label(segment_id: str | int | None) -> str:
    """Иерархическое название сегмента или строковый id."""
    if segment_id is None:
        return ""
    key = str(segment_id).strip()
    if not key:
        return ""
    return _segment_title_by_id_cached().get(key, key)


def apply_segment_labels(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Подставить segment_title в label, value остаётся segment_id."""
    labels = _segment_title_by_id_cached()
    if not labels:
        return pairs
    out: list[tuple[str, str]] = []
    for value, _old_label in pairs:
        out.append((value, labels.get(value, value)))
    return out


def invalidate_segment_label_cache() -> None:
    _segment_title_by_id_cached.cache_clear()
