# dbt-streetlight-analysis

dbt + Python project that replaces the legacy *Rental Light Customers* Excel
workbook with a queryable SQL Server data warehouse. The headline output is a
pair of fact marts estimating monthly kWh consumption — one for active rental
lights, one for non-rental streetlights — each feeding its own MDM Loss
Analysis importer.

## What this does

WGE estimates kWh usage for two populations of unmetered lights from
different upstream systems:

- **Rental lights** come from UIS billing (`CSM_Connection_Master` +
  `CSM_Equipment_Master` + `UM00403`). The seed `rate_wattage.csv` maps
  billing rate codes to lamp wattages.
- **Non-rental streetlights** come from a GIS asset inventory
  (`street_lights.csv`, sourced from engineering). Each row carries its own
  wattage and a `kwh_hr` (kWh per hour) value; substation and feeder are
  parsed from the GIS `circuit` code.
- **Hours of darkness** are sourced from the U.S. Naval Observatory for
  Westfield, MA (`hours_of_darkness_daily.csv`).

Monthly kWh:
- Rental    : `wattage × monthly_darkness_hours × fixed_mult / 1000`
- Street    : `COALESCE(kwh_hr × monthly_darkness_hours, wattage × monthly_darkness_hours / 1000)`

Each mart holds **one month** of estimates — the MDM importers take a single
month at a time. The month is chosen by the `report_month()` macro: it
defaults to the month of the run date and can be overridden with
`dbt run --vars 'report_month: <1-12>'`.

The kWh figure is the estimate for the **whole calendar month** — a forecast
from a fixed seasonal darkness profile, not a real-time or metered reading.
It does not depend on which day the pipeline runs: a run on the 1st, the
13th, or the last day of May all produce the same full-May estimate.

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
   dbt/seeds/rate_wattage.csv           ─┐ dbt seed                     │
   dbt/seeds/hours_of_darkness_daily.csv │ ────────►  <warehouse>.raw.* │
   dbt/seeds/street_lights.csv          ─┘                              │
                                                                        │  dbt run
                                                                        ▼
                                                            staging  (views)
                                                                ↓
                                                           intermediate (views)
                                                                ↓
                                                              marts  (tables)
```

## Repository layout

The repo holds two sibling sub-projects (`dbt/` and `ingest/`) plus the
`run.py` entrypoint and shared configuration at the root. The Python ingest
and dbt project each live in their own directory and share one `.env` and
one `.venv`.

```
.
├── .env                          # gitignored — real credentials
├── .env.example                  # committed template
├── run.py                        # pipeline entrypoint — sync + dbt build
├── run.ps1                        # Windows launcher for run.py
├── README.md
├── .venv/                        # gitignored — shared Python env
│
├── dbt/                          # the dbt project
│   ├── dbt_project.yml
│   ├── packages.yml              # dbt_utils
│   ├── profiles.yml              # gitignored — reads .env via env_var()
│   ├── profiles.example.yml      # committed template
│   ├── macros/                   # report_month + schema-name override
│   ├── models/
│   │   ├── staging/              # one stg_* per raw table or seed (views)
│   │   ├── intermediate/         # joins, latest-rate, monthly aggregates (views)
│   │   └── marts/                # business-facing fact tables
│   └── seeds/
│       ├── hours_of_darkness_daily.csv     # 365 rows, year-agnostic
│       ├── rate_wattage.csv                # 16 rows, rate code → wattage
│       └── street_lights.csv               # 5,706 rows, GIS asset inventory
│
└── ingest/                       # the Python loader
    ├── sync_wge.py               # WGE → warehouse.raw.* replication script
    └── ddl/
        ├── 01_create_raw_schema.sql
        └── 02_create_raw_tables.sql
```

`run.py` loads `.env` and points dbt at `dbt/` (via `DBT_PROJECT_DIR` /
`DBT_PROFILES_DIR`) before doing anything — there's no shell activation to
remember, and dbt works regardless of the current directory.

## Prerequisites

- Python 3.12 (first-time setup creates the `.venv/` with pyodbc + dbt-sqlserver)
- PowerShell on Windows
- SQL Server ODBC Driver 17 or 18
  (check with `Get-OdbcDriver | Where-Object Name -like '*SQL Server*'`)
- Read access to the WGE SQL Server instance
- `CREATE SCHEMA` / `CREATE TABLE` rights in a separate warehouse database on
  the warehouse SQL Server instance

## First-time setup

```powershell
# 1. Create the shared virtualenv and install dependencies
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt

# 2. Create your local .env from the template
Copy-Item .env.example .env
# …then edit .env to fill in WGE_* and WAREHOUSE_* values
```

Then in SSMS (or sqlcmd), connected to your warehouse DB, run once:

```
ingest\ddl\01_create_raw_schema.sql
ingest\ddl\02_create_raw_tables.sql
```

Verify the connection, then do the first full run:

```powershell
.\run.ps1 debug    # validates the warehouse connection (dbt debug)
.\run.ps1          # installs dbt_utils, syncs WGE, runs dbt build
```

## Day-to-day workflow

One command runs the whole pipeline — refresh `raw.*` from WGE, then
`dbt build` (seed → run → test, with every seed CSV reloaded):

```powershell
.\run.ps1
```

`run.ps1` forwards any arguments straight to dbt, with `.env` and the dbt
project already wired up — handy for iterating on a single model:

```powershell
.\run.ps1 run  -s fct_street_light_monthly_kwh   # rebuild one model
.\run.ps1 test -s fct_street_light_monthly_kwh   # test it
.\run.ps1 build --vars 'report_month: 3'         # build for a specific month
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
- `stg_street_lights` — GIS asset inventory; splits `circuit` into `substation` / `feeder`
- `int_connection_information` — active rental-light connections with current rate (replicates the legacy UIS active-streetlight query)
- `int_hours_of_darkness_monthly` — daily darkness summed to monthly totals
- `int_rental_light_monthly_kwh` — tall: one row per (rental connection, month)
- `int_street_light_monthly_kwh` — tall: one row per (non-rental streetlight, month); applies the kwh_hr → wattage fallback
- `fct_rental_light_monthly_kwh` — rental file: one row per active rental connection for the selected month, 8 columns, all `varchar(50)`
- `fct_street_light_monthly_kwh` — streetlight file: one row per non-rental streetlight for the selected month, 10 columns, all `varchar(50)`

Both marts filter their tall intermediate to a single month via `report_month()`.
Column shapes match the MDM Loss Analysis importers — see `docs/file_format.txt`.

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
| `street_lights.csv` | 5,706 | GIS asset inventory of WGE streetlights. Source-of-truth for non-rental kWh: `subtype_cd`, `circuit` (→ substation/feeder), `kwh_hr`, `wattage`, `light_type`, `date_installed`. Re-export the workbook to refresh. |

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
| `Dockerfile` | `python:3.12-slim` + MS ODBC Driver 17 + pinned Python deps + project source. CMD runs `run.py`. |
| `requirements.txt` | Python deps pinned to match the dev venv. |
| `.dockerignore` | Excludes `.env`, `.venv`, dbt build artifacts, and the local `profiles.yml` from the image. |
| `run.py` | Pipeline entrypoint: `sync_wge.py` → `dbt build`. The same file is used locally and in the container. |
| `docker-compose.yml` | Two services: `streetlight-ingest` (sleeps + holds env vars) and `ofelia` (runs `python /app/run.py` inside ingest on schedule). |

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
  `python /app/run.py` inside the ingest container

### Test the schedule manually

To trigger a run on demand without waiting for the cron:

```bash
docker exec streetlight-ingest python /app/run.py
```

That executes the same entrypoint ofelia would, with the same env vars.

### Logs

```bash
docker logs streetlight-ofelia                  # scheduler events
docker logs streetlight-ingest --tail 200       # last refresh output
```

## Making changes

Day-to-day maintenance falls into two categories with very different blast
radii: pure dbt changes (most common, ~90% of the time) and changes that
touch the raw ingest layer (rarer, more involved).

### Category A — dbt-only changes

Anything inside `dbt/`: model SQL, seed CSVs, tests, the mart's column shape,
the kWh formula, renaming columns, adding a dimension. None of this touches
the Python ingest or `raw.*` table schema.

**Why dbt is forgiving here:** marts are `materialized: table`, so every
`dbt run` drops and recreates them. Schema can change however you want — add
columns, rename them, change types, change formulas — with no `ALTER TABLE`
to write. Edit the model, run dbt, done.

**The cycle:**

```powershell
# 1. Edit a .sql or .csv file in dbt/

# 2. Iterate locally — your venv hits the SAME warehouse the container does
.\run.ps1 run  -s <model_name>    # rebuild just the model you changed
.\run.ps1 test -s <model_name>    # run its tests

# 3. Verify in SSMS:
#    SELECT TOP 10 * FROM <schema>.<model_name>

# 4. Commit + push to GitHub
git add dbt/
git commit -m "feat(mart): add light_type column"
git push

# 5. Rebuild + push the image with a bumped tag
docker build -t dddagostino/streetlight-ingest:1.0.1 .
docker push dddagostino/streetlight-ingest:1.0.1

# 6. Bump the tag in docker-compose.yml; scp + ssh to the server
#    Then on the server:
docker compose pull
docker compose up -d
```

**Concrete example — adding `light_type` to the streetlight mart:**

```sql
-- 1. int_street_light_monthly_kwh.sql already selects light_type from
--    stg_street_lights — no change needed there.
-- 2. In dbt/models/marts/fct_street_light_monthly_kwh.sql, add it to the
--    final SELECT:
cast(light_type as varchar(50)) as [LightType],
```

Then `dbt run -s +fct_street_light_monthly_kwh` (the `+` rebuilds upstream
deps) and check the new column in SSMS. If it looks right, commit and cut a
new image tag. Note: the MDM importers expect a fixed column set, so confirm
with the vendor before changing a mart's shape.

### Category B — schema changes in `raw.*`

This is when you need data from WGE that we're not currently fetching — a
column we ignore, or a new table entirely. The change touches both the
Python ingest and the dbt layers, so coordination is required.

**The cycle:**

```bash
# 1. Add the column to ingest/ddl/02_create_raw_tables.sql for future re-creates

# 2. ALTER the existing table in SSMS — the DDL script is idempotent (guarded
#    by IF OBJECT_ID IS NULL) and won't add new columns to existing tables:
#       ALTER TABLE raw.csm_connection_master ADD billing_class varchar(32) NULL;

# 3. Update ingest/sync_wge.py — add the column to the SELECT in SYNC_PLAN
#    and to the `columns` list (the INSERT target)

# 4. Update dbt/models/staging/_sources.yml to document the new column

# 5. Update dbt/models/staging/stg_csm_connection_master.sql to expose it

# 6. Update downstream intermediate / mart models as needed

# 7. Test locally:
.\run.ps1

# 8. Commit, rebuild image with bumped tag, push, deploy
```

Schema changes in `raw.*` are **persistent** (`ALTER TABLE`), unlike dbt
models which rebuild from scratch on every run. That's intentional: source
data should be stable, derived data should be cheap to rebuild.

### Image tag discipline

Never reuse a tag. Docker Hub caches by tag, the deployment server may have a
cached layer, and re-pushing the same tag turns minor issues into debugging
nightmares.

| Change kind | Tag bump |
|---|---|
| Bug fix in a model | `1.0.0` → `1.0.1` |
| New column / feature in the mart | `1.0.1` → `1.1.0` |
| Breaking change (column MDM consumes is renamed/removed) | `1.1.0` → `2.0.0` |

Two places to update the tag for each release:

- The `docker build -t dddagostino/streetlight-ingest:<tag> .` command
- The `image:` line in `docker-compose.yml`

### Gotchas

1. **Local `dbt run` writes to the same database as production.** There's
   currently a single `dev` target pointed at `StreetLightAnalytics`.
   Experimental changes locally affect anything reading the mart in real
   time. Mitigation when needed: add a second target in `profiles.yml` (e.g.
   `dev` → `StreetLightAnalytics_Dev`, `prod` → `StreetLightAnalytics`) and
   run `dbt run --target dev` for iteration. The container always uses `prod`.

2. **Seeds reload on every run.** `run.py` calls `dbt build`, which reloads
   every CSV in `dbt/seeds/` — locally and in the container alike. Edit a
   seed and the next run picks it up automatically. (A seed *column-type*
   change in `dbt_project.yml` still needs `.\run.ps1 build --full-refresh`.)

3. **GitHub is the source of truth for code; Docker Hub is for what's
   deployed.** Pushing to GitHub doesn't update production. The image has to
   be rebuilt, pushed, pulled by the server, and the container recreated for
   any code change to actually take effect on the 1st-of-the-month run.

4. **`raw.*` table schema is owned by the Python DDL, not by dbt.** If you
   `DROP TABLE` one of those manually, the next `sync_wge.py` run will fail
   loudly. Re-run the DDL script first, then the ingest.

## Secrets policy

- `.env` and `profiles.yml` are gitignored. Never commit them.
- `.env.example` and `profiles.example.yml` are templates with no real values.
- If a credential is ever accidentally pasted into a chat, log, or commit,
  rotate it with the DBA before doing anything else.
