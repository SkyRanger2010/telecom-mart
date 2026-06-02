FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DJANGO_SETTINGS_MODULE=martdash.settings PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY martdash/ /app/

RUN python manage.py collectstatic --no-input --clear

EXPOSE 8080

CMD ["sh", "-c", "python manage.py migrate --noinput && exec gunicorn martdash.wsgi:application --bind 0.0.0.0:8080 --workers 2 --threads 2 --timeout 120"]
