"""Replicate selected WGE (UIS) tables into the warehouse raw schema.

Reads connection settings from .env at the repo root, pulls three rental-light /
streetlight tables from WGE, and writes them to <warehouse>.raw.* on the
warehouse SQL Server. Run before `dbt run` to refresh the raw layer.

Usage:
    . .\\Activate.ps1            # in PowerShell, dot-sourced
    python ingest\\sync_wge.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pyodbc

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# .env loading (no external dependency on python-dotenv)
# ---------------------------------------------------------------------------
def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        val = val.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), val)


def require(var: str) -> str:
    val = os.environ.get(var, "").strip()
    if not val:
        sys.exit(
            f"ERROR: required environment variable {var} is not set.\n"
            f"  Ensure {REPO_ROOT / '.env'} exists and contains {var}=..."
        )
    return val


def conn_str(server: str, database: str, user: str = "", password: str = "") -> str:
    driver = os.environ.get("SQLSERVER_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    trust  = os.environ.get("SQLSERVER_TRUST_CERT", "yes")
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        "Encrypt=yes",
        f"TrustServerCertificate={trust}",
    ]
    if user:
        # SQL authentication
        parts += [f"UID={user}", f"PWD={password}"]
    else:
        # Windows integrated authentication
        parts.append("Trusted_Connection=yes")
    return ";".join(parts) + ";"


# ---------------------------------------------------------------------------
# What to sync: SELECT from WGE -> INSERT into warehouse.raw.<target>
# Column names are snake_cased here so the warehouse raw layer is clean.
# ---------------------------------------------------------------------------
SYNC_PLAN = [
    {
        "target": "csm_connection_master",
        "columns": [
            "location_id", "equipment_id", "connection_date",
            "disconnection_date", "connection_status", "fixed_mult",
        ],
        "select_sql": """
            SELECT
                CAST(Location              AS varchar(32))  AS location_id,
                CAST(Equipment             AS varchar(32))  AS equipment_id,
                CAST([Connection Date]     AS date)         AS connection_date,
                CAST([Disconnection Date]  AS date)         AS disconnection_date,
                CAST([Connection Status]   AS varchar(32))  AS connection_status,
                CAST(Consumption           AS int)          AS fixed_mult
            FROM dbo.CSM_Connection_Master
            WHERE LEFT(Equipment, 2) = 'zz'
        """,
    },
    {
        "target": "csm_equipment_master",
        "columns": ["equipment_id", "equip_class"],
        "select_sql": """
            SELECT
                CAST(Equipment         AS varchar(32))  AS equipment_id,
                CAST([Equipment Class] AS varchar(64))  AS equip_class
            FROM dbo.CSM_Equipment_Master
            WHERE LEFT(Equipment, 2) = 'zz'
        """,
    },
    {
        "target": "um00403",
        "columns": ["location_id", "equipment_id", "connect_seq", "rate1"],
        "select_sql": """
            SELECT
                CAST(umLocationID  AS varchar(32))  AS location_id,
                CAST(umEquipmentID AS varchar(32))  AS equipment_id,
                CAST(umConnectSeq  AS int)          AS connect_seq,
                CAST(UMTAR1        AS varchar(16))  AS rate1
            FROM dbo.UM00403
            WHERE LEFT(umEquipmentID, 2) = 'zz'
        """,
    },
]


def main() -> int:
    load_env(REPO_ROOT / ".env")

    wge_server   = require("WGE_SERVER")
    wge_database = os.environ.get("WGE_DATABASE", "WGE")
    wge_user     = os.environ.get("WGE_USER", "")
    wge_password = os.environ.get("WGE_PASSWORD", "")

    wh_server   = require("WAREHOUSE_SERVER")
    wh_database = require("WAREHOUSE_DATABASE")
    wh_user     = os.environ.get("WAREHOUSE_USER", "")
    wh_password = os.environ.get("WAREHOUSE_PASSWORD", "")

    wge_cs = conn_str(wge_server, wge_database, wge_user, wge_password)
    wh_cs  = conn_str(wh_server,  wh_database,  wh_user,  wh_password)

    print(f"WGE source     : {wge_server} / {wge_database} "
          f"({'SQL auth: ' + wge_user if wge_user else 'Windows auth'})")
    print(f"Warehouse dest : {wh_server} / {wh_database} "
          f"({'SQL auth: ' + wh_user if wh_user else 'Windows auth'})")
    print()

    with pyodbc.connect(wge_cs) as src, pyodbc.connect(wh_cs) as dst:
        for task in SYNC_PLAN:
            target = task["target"]
            cols   = task["columns"]
            print(f"-- syncing raw.{target}")

            rows = src.cursor().execute(task["select_sql"]).fetchall()
            print(f"   fetched {len(rows)} rows from WGE")

            cur = dst.cursor()
            cur.execute(f"DELETE FROM raw.{target}")  # full-replace each run
            placeholders = ",".join("?" for _ in cols)
            cur.fast_executemany = True
            cur.executemany(
                f"INSERT INTO raw.{target} ({','.join(cols)}) VALUES ({placeholders})",
                [tuple(r) for r in rows],
            )
            dst.commit()
            print(f"   loaded {len(rows)} rows into raw.{target}\n")

    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
