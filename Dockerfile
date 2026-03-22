FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bosch_local_probe.py /app/bosch_local_probe.py
COPY bosch_mqtt_bridge.py /app/bosch_mqtt_bridge.py

RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "/app/bosch_mqtt_bridge.py"]
