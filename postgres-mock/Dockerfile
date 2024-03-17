FROM python:3.11-slim-buster

ENV DBT_PROFILES_DIR=/app/dbt_redshift_stats

WORKDIR /app

# Update and install dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y --no-install-recommends build-essential libpq-dev git && \
    apt-get clean && \
    apt-get autoclean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Upgrade pip and install Python dependencies
RUN python -m pip install --upgrade pip

# Install Python dependencies
COPY postgres-mock/requirements.txt .
RUN python -m pip install -r requirements.txt

# Copy everything from the postgres-mock directory
COPY postgres-mock/ .

# Copy the dbt project
COPY dbt_redshift_stats/ /app/dbt_redshift_stats

# Copy profiles to dbt project
COPY dbt_redshift_stats/profiles.yml /app/dbt_redshift_stats/profiles.yml

# Copy the scripts directory to a location inside the container
COPY scripts/postgres-mock /app/scripts

COPY postgres-mock/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]