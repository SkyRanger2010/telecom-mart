"""Маршруты дашборда: главная, API (абоненты/финансы/NL2SQL), drilldown по витринам."""
from django.urls import path

from vitrina import views

urlpatterns = [
    path("health/", views.health, name="health"),
    path("api/subscriber-base/", views.subscriber_api, name="subscriber_api"),
    path("api/financial/", views.financial_api, name="financial_api"),
    path("api/query-dashboard/", views.query_dashboard_api, name="query_dashboard_api"),
    path("api/chat/nl2sql/", views.nl2sql_chat, name="nl2sql_chat"),
    path("financial/", views.financial_dashboard, name="financial_dashboard"),
    path("query/", views.query_dashboard, name="query_dashboard"),
    path("v/<str:table>/", views.detail, name="vitrina_detail"),
    path("", views.index, name="vitrina_index"),
]
