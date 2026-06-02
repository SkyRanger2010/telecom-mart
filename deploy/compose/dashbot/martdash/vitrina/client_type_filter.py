"""Группировка и API-структура фильтра «Тип клиента» (подпись client_type_title)."""

from __future__ import annotations

from typing import Any

CLIENT_TYPE_CODES = frozenset({"person", "org", "ip", "unknown"})

REGION_GROUP_DEFS: tuple[dict[str, str], ...] = (
    {"id": "rf", "label": "РФ"},
    {"id": "non_resident", "label": "Нерезиденты"},
    {"id": "ukr", "label": "УКР"},
)

QUICK_PICK_DEFS: tuple[dict[str, str], ...] = (
    {"id": "person", "label": "Физлица", "code": "person"},
    {"id": "org", "label": "Юрлица", "code": "org"},
    {"id": "ip", "label": "ИП", "code": "ip"},
)


def is_client_type_code(value: str) -> bool:
    return value.strip().lower() in CLIENT_TYPE_CODES


def classify_region(title: str) -> str:
    """Региональная группа по префиксу подписи client_property.title."""
    t = (title or "").strip()
    if not t:
        return "other"
    if t.upper().startswith("УКР"):
        return "ukr"
    low = t.casefold()
    if low.startswith("нерезидент"):
        return "non_resident"
    if t.startswith("РФ") or t.upper().startswith("РФ"):
        return "rf"
    return "other"


def filter_row_key(client_type: str, title: str) -> str:
    """Ключ фильтра в URL: подпись, иначе код client_type."""
    title = (title or "").strip()
    if title:
        return title
    return (client_type or "").strip()


def build_client_type_filter_options(
    rows: list[tuple[str, str]],
    *,
    selected: list[str] | None = None,
) -> dict[str, Any]:
    """
  rows: (client_type code, label) — label обычно client_type_title.
  Возвращает плоский список client_types и сгруппированную структуру для UI.
  """
    by_key: dict[str, dict[str, str]] = {}
    for code, label in rows:
        code = (code or "").strip().lower()
        label = (label or "").strip() or code
        if not code and not label:
            continue
        key = filter_row_key(code, label)
        if not key or key in by_key:
            continue
        by_key[key] = {"value": key, "label": label, "code": code or "unknown"}

    for s in selected or []:
        sk = (s or "").strip()
        if sk and sk not in by_key:
            code = sk.lower() if is_client_type_code(sk) else "unknown"
            by_key[sk] = {"value": sk, "label": sk, "code": code}

    items = sorted(by_key.values(), key=lambda x: x["label"].casefold())

    groups_map: dict[str, list[dict[str, str]]] = {g["id"]: [] for g in REGION_GROUP_DEFS}
    groups_map["other"] = []
    for it in items:
        region = classify_region(it["label"])
        bucket = region if region in groups_map else "other"
        groups_map[bucket].append(it)

    groups: list[dict[str, Any]] = []
    for gdef in REGION_GROUP_DEFS:
        gid = gdef["id"]
        group_items = groups_map.get(gid) or []
        if not group_items:
            continue
        groups.append(
            {
                "id": gid,
                "label": gdef["label"],
                "items": group_items,
            }
        )
    other_items = groups_map.get("other") or []
    if other_items:
        groups.append({"id": "other", "label": "Прочее", "items": other_items})

    return {
        "client_types": items,
        "client_type_groups": groups,
        "client_type_quick_picks": [dict(p) for p in QUICK_PICK_DEFS],
    }


def client_types_where_sql(
    table: str,
    values: list[str],
    *,
    has_client_type: bool,
    has_client_type_title: bool,
    escape_str: Any,
) -> str:
    """WHERE-фрагмент: коды person/org/ip → client_type; подписи → client_type_title."""
    if not values or not has_client_type:
        return ""
    codes = [v.strip().lower() for v in values if is_client_type_code(v)]
    titles = [v.strip() for v in values if v.strip() and not is_client_type_code(v)]
    parts: list[str] = []
    if codes:
        lit = ",".join(f"'{escape_str(x)}'" for x in codes)
        parts.append(f"`client_type` IN ({lit})")
    if titles and has_client_type_title:
        lit = ",".join(f"'{escape_str(x)}'" for x in titles)
        parts.append(f"`client_type_title` IN ({lit})")
    elif titles and not has_client_type_title:
        lit = ",".join(f"'{escape_str(x)}'" for x in titles)
        parts.append(f"`client_type` IN ({lit})")
    if not parts:
        return ""
    if len(parts) == 1:
        return f" AND ({parts[0]}) "
    return f" AND ({' OR '.join(parts)}) "
