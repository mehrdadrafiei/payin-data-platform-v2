ARG HUB_REGISTRY=docker.io
FROM ${HUB_REGISTRY}/library/python:3.12-slim
ARG PIP_INDEX_URL=
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /app
RUN if [ -n "$PIP_INDEX_URL" ]; then \
      pip install --no-cache-dir --index-url "$PIP_INDEX_URL" dbt-core==1.8.9 dbt-clickhouse==1.8.9; \
    else \
      pip install --no-cache-dir dbt-core==1.8.9 dbt-clickhouse==1.8.9; \
    fi
COPY scheduler.py /app/scheduler.py
COPY dbtw.sh /usr/local/bin/dbtw
RUN chmod +x /usr/local/bin/dbtw
# the dbt project is mounted at /app/dbt (see docker-compose.yml)
CMD ["python", "/app/scheduler.py"]
