{% docs __overview__ %}

# WGE Streetlight & Rental-Light kWh Analysis

This project estimates **monthly kWh usage** for WGE's unmetered lighting and
delivers it to MDM Loss Analysis. It replaces a legacy Excel workbook with a
repeatable, testable SQL + dbt pipeline.

The output is **two fact tables**, each loaded by its own MDM importer:

- `fct_rental_light_monthly_kwh` — rental lights
- `fct_street_light_monthly_kwh` — non-rental streetlights

---

## How usage is calculated

Every light here is unmetered, so usage is **estimated, not measured**. The
estimate is the same idea for both populations:

> **power draw  ×  how long the light runs**

"How long the light runs" is the **hours of darkness** — these lights run from
dusk to dawn.

### Hours of darkness

The `hours_of_darkness_daily` seed holds the duration of darkness for each of
the 365 days of the year at Westfield, MA (U.S. Naval Observatory). It is
**year-agnostic** — the same seasonal profile applies to any year.

`int_hours_of_darkness_monthly` sums those daily values into one
`monthly_darkness_hours` total per calendar month (1–12). This is the
"how long the light runs" term in every estimate below.

### Rental-light kWh

Rental lights come from UIS billing. Each connection's wattage is looked up
from the `rate_wattage` seed using its current rate code.

```
est_kwh = wattage × monthly_darkness_hours × fixed_mult / 1000
```

`fixed_mult` is the number of lights sharing one connection. Only **active**
connections are included.

### Streetlight kWh

Streetlights come from the GIS asset inventory (`street_lights` seed). A row
may carry a `kwh_hr` value (kWh per hour), a nameplate `wattage`, or both. The
model uses whichever is reliable:

```
est_kwh =
    kwh_hr  × monthly_darkness_hours          when kwh_hr is a valid per-hour
                                              value (0 < kwh_hr <= 1)

    wattage × monthly_darkness_hours / 1000   otherwise — when kwh_hr is 0,
                                              null, or out of range

    NULL                                      when neither input is usable
```

**Why the `0 < kwh_hr <= 1` guard:** a real kWh-per-hour figure is a small
fraction (e.g. 0.039 for a 39 W lamp). Some source rows have wattage
mis-keyed into the `kwh_hr` column (values like 39 or 129) — the upper bound
rejects those. A `kwh_hr` of 0 or null is treated as *not measured* (not
"true zero"), so the model falls back to the nameplate wattage. In the
current data every in-scope streetlight has a usable wattage, so all of them
receive an estimate.

### Scope

| Population | Included |
|---|---|
| Rental lights | Active connections only |
| Streetlights  | `subtype_cd` of `Streetlight` or `Decorative Streetlight` — off-street lights are excluded |

---

## One month per run

The MDM importers take a single month at a time, so each mart holds **one
month** and has no Month column. The month is chosen by the `report_month()`
macro:

- **Default** — the month *of* the run date. The pipeline runs on the 1st of
  each month, so an unattended run produces that month's estimated usage
  (a June 1 run produces June data).
- **Override** — `dbt run --vars 'report_month: <1-12>'`.

---

## The pipeline

```
WGE UIS  ──sync_wge.py──►  raw.*  ─┐
                                   ├──►  staging  ──►  intermediate  ──►  marts
seeds    ──dbt seed─────►  raw.*  ─┘     (views)        (views)           (tables)
```

| Layer | What it does |
|---|---|
| **raw** | WGE tables replicated by `sync_wge.py`; seed CSVs loaded by `dbt seed` |
| **staging** (`stg_*`) | Thin pass-through / light cleanup, one model per source |
| **intermediate** (`int_*`) | Scope filters, joins, and the kWh calculation |
| **marts** (`fct_*`) | One month, formatted for the MDM importers |

The whole process runs with one command — `run.py` (locally via `run.ps1`,
in the container via the monthly schedule): it refreshes `raw.*` from WGE,
then runs `dbt build` (seed → run → test).

{% enddocs %}
