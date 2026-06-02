from django.urls import include, path

urlpatterns = [
    path("", include("vitrina.urls")),
]
