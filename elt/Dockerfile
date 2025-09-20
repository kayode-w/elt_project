# FROM postgres:17.5

# RUN apt-get update && apt-get install -y python3 python3-pip

# COPY elt_script.py .

# CMD ["python3", "elt_script.py"]


FROM python:3.10-slim

# Install required dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    lsb-release \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Add PostgreSQL APT repo key
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgres.gpg

# Add PostgreSQL APT repo
RUN echo "deb [signed-by=/usr/share/keyrings/postgres.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# Install PostgreSQL 17 client
RUN apt-get update && apt-get install -y postgresql-client-17

# Copy the ELT script
COPY elt_script.py .

# Run the script
CMD ["python", "elt_script.py"]


