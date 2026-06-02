"""Данные для общего шаблона (боковое меню витрин)."""

from __future__ import annotations

def vitrina_nav(request):
    rm = getattr(request, "resolver_match", None)
    name = rm.url_name if rm else ""
    return {
        "nav_subscriber_home": name == "vitrina_index",
        "nav_financial": name == "financial_dashboard",
        "nav_query_dashboard": name == "query_dashboard",
    }
