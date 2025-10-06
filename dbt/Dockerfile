# FROM python:3.11-slim

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     git build-essential ca-certificates && \
#     rm -rf /var/lib/apt/lists/*

# # Pin versions you use (adjust if needed)
# RUN pip install --no-cache-dir \
#     dbt-core==1.7.15 \
#     dbt-postgres==1.7.15

# WORKDIR /dbt
# ENTRYPOINT ["dbt"]

FROM python:3.11-slim

# system deps (git is helpful for packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates build-essential \
  && rm -rf /var/lib/apt/lists/*

# dbt (pin to your project)
RUN pip install --no-cache-dir \
    dbt-core==1.7.15 \
    dbt-postgres==1.7.15

# optional: psql client (handy for debug; can skip)
# RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*

WORKDIR /dbt
# default entrypoint is dbt; Airflow/compose can override command (e.g. "build")
ENTRYPOINT ["dbt"]
