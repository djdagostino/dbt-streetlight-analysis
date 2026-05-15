#!/usr/bin/env python3
"""Streetlight pipeline entrypoint — one command runs the whole process.

No arguments:
    Full pipeline — ensure dbt packages are installed, replicate WGE into
    raw.* (ingest/sync_wge.py), then `dbt build` (seed -> run -> test in
    dependency order). `dbt build` reloads every seed CSV, so changes to
    any seed are picked up automatically.

With arguments:
    Forwarded straight to dbt, with .env and DBT_*_DIR already set —
    e.g. `python run.py debug`, `python run.py run -s int_street_light_monthly_kwh`.

The same file is used locally (via run.ps1) and inside the container
(Dockerfile CMD), so there is one orchestration script, not two.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DBT_DIR = REPO_ROOT / "dbt"


def load_env(path: Path) -> None:
    """Load KEY=VALUE lines from .env into os.environ (pre-set vars win)."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


def dbt_exe() -> str:
    """The dbt console script living next to this Python interpreter."""
    name = "dbt.exe" if os.name == "nt" else "dbt"
    candidate = Path(sys.executable).with_name(name)
    return str(candidate) if candidate.exists() else name


def run(cmd: list[str]) -> None:
    """Echo and run a command; abort the whole script on a non-zero exit."""
    print(f"\n$ {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main(argv: list[str]) -> int:
    load_env(REPO_ROOT / ".env")
    # dbt resolves the project + profiles from these — no flags, any cwd.
    os.environ.setdefault("DBT_PROJECT_DIR", str(DBT_DIR))
    os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

    dbt = dbt_exe()

    # Passthrough mode: hand the args to dbt with the environment ready.
    if argv:
        run([dbt, *argv])
        return 0

    # Full pipeline.
    print("=" * 65)
    print("  Streetlight pipeline - sync WGE -> raw.*, then dbt build")
    print("=" * 65)

    # Only fetch packages when missing, so the monthly run never depends on
    # dbt Hub being reachable (the image bakes them in at build time).
    if not (DBT_DIR / "dbt_packages").exists():
        print("\n==> dbt packages missing — installing")
        run([dbt, "deps"])

    print("\n==> Step 1/2: WGE -> raw.* via ingest/sync_wge.py")
    run([sys.executable, str(REPO_ROOT / "ingest" / "sync_wge.py")])

    print("\n==> Step 2/2: dbt build (seed -> run -> test)")
    run([dbt, "build"])

    print("\n" + "=" * 65)
    print("  Completed successfully")
    print("=" * 65)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
