FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Pin versions you use (adjust if needed)
RUN pip install --no-cache-dir \
    dbt-core==1.7.15 \
    dbt-postgres==1.7.15

WORKDIR /dbt
ENTRYPOINT ["dbt"]