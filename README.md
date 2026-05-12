# dbt-streetlight-analysis

dbt + Python project that replaces the legacy *Rental Light Customers* Excel
workbook with a queryable SQL Server data warehouse. The headline output is a
fact mart estimating monthly kWh consumption for every active rental
streetlight in WGE's UIS system — feeding MDM Loss Analysis.

## What this does

WGE estimates kWh usage for unmetered rental streetlights using three inputs:

1. **Connection information** — active light/customer records from UIS
   (`WGE.dbo.CSM_Connection_Master` + `CSM_Equipment_Master` + `UM00403`)
2. **Hours of darkness** — daily duration of darkness at Westfield, MA, per the
   U.S. Naval Observatory
3. **Wattage by rate code** — a small reference table mapping billing rate
   codes to lamp wattages

Estimated monthly kWh = `wattage × hours_of_darkness_in_month × number_of_lights / 1000`.

This project rebuilds the logic in SQL + dbt so it's repeatable, testable, and
queryable from any BI tool.

## Architecture

```
   WGE SQL Server instance              Warehouse SQL Server instance
   (read-only source)                   (this project's target)

   WGE.dbo.CSM_Connection_Master
   WGE.dbo.CSM_Equipment_Master  ──┐
   WGE.dbo.UM00403                 │
                                   │  ingest/sync_wge.py
                                   ▼
                              <warehouse>.raw.csm_connection_master
                              <warehouse>.raw.csm_equipment_master      ┐
                              <warehouse>.raw.um00403                   │
                                                                        │
   dbt/seeds/rate_wattage.csv          ─┐  dbt seed                     │
   dbt/seeds/hours_of_darkness_daily.csv │  ────────►  <warehouse>.raw.*│
                                        ─┘                              │
                                                                        │  dbt run
                                                                        ▼
                                                            staging  (views)
                                                                ↓
                                                           intermediate (views)
                                                                ↓
                                                              marts  (tables)
```

## Repository layout

The repo holds two sibling sub-projects (`dbt/` and `ingest/`) plus shared
configuration at the root. The Python ingest and dbt project each live in
their own directory and share one `.env`, one `.venv`, and one `Activate.ps1`.

```
.
├── .env                          # gitignored — real credentials
├── .env.example                  # committed template
├── Activate.ps1                  # PS bootstrap: venv + .env + DBT_*_DIR
├── README.md
├── .venv/                        # gitignored — shared Python env
│
├── dbt/                          # the dbt project
│   ├── dbt_project.yml
│   ├── packages.yml              # dbt_utils
│   ├── profiles.yml              # gitignored — reads .env via env_var()
│   ├── profiles.example.yml      # committed template
│   ├── models/
│   │   ├── staging/              # one stg_* per raw table or seed (views)
│   │   ├── intermediate/         # joins, latest-rate, monthly aggregates (views)
│   │   └── marts/                # business-facing tables (TODO)
│   └── seeds/
│       ├── hours_of_darkness_daily.csv     # 365 rows, year-agnostic
│       └── rate_wattage.csv                # 16 rows, rate code → wattage
│
└── ingest/                       # the Python loader
    ├── sync_wge.py               # WGE → warehouse.raw.* replication script
    └── ddl/
        ├── 01_create_raw_schema.sql
        └── 02_create_raw_tables.sql
```

`Activate.ps1` sets `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` to `dbt\`, so
`dbt run` / `dbt seed` / `dbt test` work from anywhere in the repo — no need
to `cd dbt`.

## Prerequisites

- Python 3.12 with the project venv at `.venv/` (pyodbc + dbt-sqlserver installed)
- PowerShell on Windows
- SQL Server ODBC Driver 17 or 18
  (check with `Get-OdbcDriver | Where-Object Name -like '*SQL Server*'`)
- Read access to the WGE SQL Server instance
- `CREATE SCHEMA` / `CREATE TABLE` rights in a separate warehouse database on
  the warehouse SQL Server instance

## First-time setup

```powershell
# 1. Create your local .env from the template
Copy-Item .env.example .env
# …then edit .env to fill in WGE_* and WAREHOUSE_* values

# 2. Activate the session (loads venv + .env, sets DBT_PROFILES_DIR)
. .\Activate.ps1
```

Then in SSMS (or sqlcmd), connected to your warehouse DB, run once:

```
ingest\ddl\01_create_raw_schema.sql
ingest\ddl\02_create_raw_tables.sql
```

Verify and bootstrap dbt:

```powershell
dbt debug          # validates the warehouse connection
dbt deps           # installs dbt_utils
dbt seed           # loads rate_wattage and hours_of_darkness_daily
```

## Day-to-day workflow

```powershell
. .\Activate.ps1                # once per shell session

python ingest\sync_wge.py       # refresh raw.* from WGE (seconds for ~300 rows)
dbt run                         # rebuild staging → intermediate → marts
dbt test                        # run not_null / unique / relationships tests
```

## Configuration reference (.env)

All settings live in `.env`. They're read by both the Python script and
`profiles.yml` (via `{{ env_var('NAME') }}`).

| Variable | Used by | Notes |
|---|---|---|
| `WGE_SERVER` | sync_wge.py | `host`, `host\instance`, or `ip,port` |
| `WGE_DATABASE` | sync_wge.py | Defaults to `WGE` |
| `WGE_USER` / `WGE_PASSWORD` | sync_wge.py | Empty = Windows integrated auth |
| `WAREHOUSE_SERVER` | sync_wge.py + dbt | Target SQL Server instance |
| `WAREHOUSE_DATABASE` | sync_wge.py + dbt | Target database in that instance |
| `WAREHOUSE_USER` / `WAREHOUSE_PASSWORD` | sync_wge.py + dbt | Empty = Windows auth |
| `SQLSERVER_ODBC_DRIVER` | both | e.g. `ODBC Driver 17 for SQL Server` |
| `SQLSERVER_TRUST_CERT` | both | `yes` for internal SQL with self-signed certs |

## Model layers

| Layer | Purpose | Materialization |
|---|---|---|
| **raw** (schema) | Tables written by `sync_wge.py` and `dbt seed`. Not modeled by dbt. | tables |
| **staging** (`stg_*`) | Thin pass-through — one model per raw table/seed. | view |
| **intermediate** (`int_*`) | Joins, latest-rate selection, monthly aggregates. | view |
| **marts** (`fct_*` / `dim_*`) | Business-facing facts and dimensions. | table |

### Current models

- `stg_csm_connection_master`, `stg_csm_equipment_master`, `stg_um00403` — UIS sources
- `stg_rate_wattage`, `stg_hours_of_darkness_daily` — reference seeds
- `int_connection_information` — active rental-light connections with current rate (replicates the legacy UIS active-streetlight query)
- `int_hours_of_darkness_monthly` — daily darkness summed to monthly totals

### Planned

- `fct_rental_light_monthly_kwh` — one row per `(location_id, year_month)` with
  `wattage × hours_of_darkness × fixed_mult / 1000` as `est_kwh`. Filtered to
  months where the connection was active.

## Data sources

### WGE (read-only source)

Pulled by `sync_wge.py`, filtered to `LEFT(Equipment, 2) = 'zz'` at ingest:

| Table | Purpose |
|---|---|
| `dbo.CSM_Connection_Master` | Connection facts: Location, Equipment, dates, status, light count (`Consumption` column reused as multiplier) |
| `dbo.CSM_Equipment_Master` | Equipment Class lookup |
| `dbo.UM00403` | Rate-code history (`UMTAR1`) per equipment; latest by `umConnectSeq` |

### Seeds (this repo)

| Seed | Rows | Purpose |
|---|---|---|
| `rate_wattage.csv` | 16 | Maps billing rate codes (e.g. `E0250C`) to lamp wattage + light type |
| `hours_of_darkness_daily.csv` | 365 | Daily duration of darkness at Westfield MA (N 42°07′, W 72°45′), U.S. Naval Observatory. Static; applies to any year. |

## Conventions

- **Snake_case columns throughout** — `sync_wge.py` renames PascalCase /
  bracketed source columns at SELECT time, so the warehouse `raw` layer is
  already clean. Staging models are near-trivial pass-throughs.
- **`zz` equipment filter at ingest** — only rental-light / streetlight
  equipment is replicated. Downstream models assume that scope.
- **`Active` status filter in intermediate, not staging** — staging keeps
  full history; `int_connection_information` is the first place it's narrowed
  to active connections.

## Production deployment (Docker + ofelia)

For monthly unattended runs, the ingest + dbt pipeline is packaged into a
single Docker image and triggered by `ofelia` (docker-native cron) on the 1st
of each month.

### Files involved

| File | Purpose |
|---|---|
| `Dockerfile` | `python:3.12-slim` + MS ODBC Driver 17 + pinned Python deps + project source. CMD runs `ingest/run.sh`. |
| `requirements.txt` | Python deps pinned to match the dev venv. |
| `.dockerignore` | Excludes `.env`, `.venv`, dbt build artifacts, and the local `profiles.yml` from the image. |
| `ingest/run.sh` | Container entrypoint: `sync_wge.py` → `dbt seed` → `dbt run` → `dbt test`. |
| `docker-compose.yml` | Two services: `streetlight-ingest` (sleeps + holds env vars) and `ofelia` (runs `bash /app/ingest/run.sh` inside ingest on schedule). |

### Build and push the image

From the repo root on your dev machine:

```bash
docker build -t dddagostino/streetlight-ingest:1.0.0 .
docker push dddagostino/streetlight-ingest:1.0.0
```

Bump the tag (e.g. `1.0.1`) for any subsequent release and update
`docker-compose.yml` to match before deploying.

### Deploy on the CMS Linux server

```bash
# 1. Copy deployment files to the server
scp docker-compose.yml <user>@<server>:/home/appdev/streetlight-ingest/docker-compose.yml
scp .env                <user>@<server>:/home/appdev/streetlight-ingest/.env

# 2. On the server
cd /home/appdev/streetlight-ingest
docker login
docker compose pull
docker compose up -d
```

After `docker compose up -d`:

- `streetlight-ingest` is running `sleep infinity` (holds env + filesystem)
- `streetlight-ofelia` is watching the docker socket
- On `0 0 6 1 * *` (06:00 UTC on the 1st of each month), ofelia executes
  `bash /app/ingest/run.sh` inside the ingest container

### Test the schedule manually

To trigger a run on demand without waiting for the cron:

```bash
docker exec streetlight-ingest bash /app/ingest/run.sh
```

That executes the same script ofelia would, with the same env vars.

### Logs

```bash
docker logs streetlight-ofelia                  # scheduler events
docker logs streetlight-ingest --tail 200       # last refresh output
```

## Secrets policy

- `.env` and `profiles.yml` are gitignored. Never commit them.
- `.env.example` and `profiles.example.yml` are templates with no real values.
- If a credential is ever accidentally pasted into a chat, log, or commit,
  rotate it with the DBA before doing anything else.
