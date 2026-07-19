FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN groupadd --system forwarder && useradd --system --gid forwarder --home /app forwarder
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY config.yaml rules.yaml ./
RUN mkdir -p /data && chown -R forwarder:forwarder /app /data && chmod 0700 /data
USER forwarder
CMD ["python", "-m", "src.main"]
