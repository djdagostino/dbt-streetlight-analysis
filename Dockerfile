# Pin to Debian 12 (bookworm) explicitly — `python:3.12-slim` now defaults to
# Debian 13 (trixie), for which Microsoft has not yet published an mssql-tools
# repo. Bookworm is supported and we'll stick with it for the foreseeable future.
FROM python:3.12-slim-bookworm

# ---------------------------------------------------------------------------
# OS deps: Microsoft ODBC Driver 17 for SQL Server (matches WGE driver version)
# Uses Microsoft's official packages-microsoft-prod.deb installer, which drops
# the keyring at /usr/share/keyrings/microsoft-prod.gpg and the sources file at
# /etc/apt/sources.list.d/mssql-release.list — matching where modern apt's
# `signed-by=` references expect them.
# ---------------------------------------------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -sSL -o /tmp/packages-microsoft-prod.deb \
        https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
 && dpkg -i /tmp/packages-microsoft-prod.deb \
 && rm /tmp/packages-microsoft-prod.deb \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 unixodbc \
 && apt-get purge -y curl \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Python deps
# ---------------------------------------------------------------------------
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Project source (run.py + ingest + dbt). profiles.yml is NOT copied (gitignored
# / in .dockerignore); we promote profiles.example.yml to profiles.yml below
# since they're functionally identical (both env_var-driven).
# ---------------------------------------------------------------------------
COPY run.py /app/run.py
COPY ingest /app/ingest
COPY dbt    /app/dbt
RUN cp /app/dbt/profiles.example.yml /app/dbt/profiles.yml

# Pre-install dbt packages at build time so each run starts fast
WORKDIR /app/dbt
RUN dbt deps

# ---------------------------------------------------------------------------
# Runtime: dbt finds the project + profiles via env vars (no flags needed)
# ---------------------------------------------------------------------------
ENV DBT_PROJECT_DIR=/app/dbt
ENV DBT_PROFILES_DIR=/app/dbt
ENV PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["python", "/app/run.py"]
