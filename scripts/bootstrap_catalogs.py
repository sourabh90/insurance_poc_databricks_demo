#!/usr/bin/env python3
"""
Pre-deploy bootstrap — creates Unity Catalog catalogs the bundle references.

DAB validates that each pipeline's `catalog:` exists when the pipeline
resource is created. The catalog-creation notebook
(src/notebooks/setup_catalogs.py) only runs AFTER `bundle deploy`
succeeds — so the deploy fails with
    "cannot create pipeline: Catalog 'gold_dev' does not exist"
unless catalogs are pre-created. This script breaks the chicken-and-egg.

Run this BEFORE `databricks bundle deploy` for any new target/workspace.

Usage:
    # Local (uses a CLI profile from ~/.databrickscfg)
    python scripts/bootstrap_catalogs.py --target dev --profile <profile-name>

    # CI (auth via DATABRICKS_HOST + DATABRICKS_TOKEN env vars)
    python scripts/bootstrap_catalogs.py --target dev
"""
from __future__ import annotations

import argparse
import subprocess
import sys

LAYERS = ("bronze", "silver", "gold", "monitoring")


def databricks(*args: str, profile: str | None) -> subprocess.CompletedProcess:
    cmd = ["databricks", *args]
    if profile:
        cmd += ["--profile", profile]
    return subprocess.run(cmd, capture_output=True, text=True)


def ensure_catalog(name: str, profile: str | None) -> None:
    if databricks("catalogs", "get", name, profile=profile).returncode == 0:
        print(f"  exists  {name}")
        return
    result = databricks("catalogs", "create", name, profile=profile)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "Default Storage" in stderr:
            sys.exit(
                f"  failed  {name}: workspace uses Default Storage which "
                f"requires UI-based catalog creation.\n"
                f"          Create '{name}' once in the Databricks UI "
                f"(Catalog Explorer -> Create catalog), then re-run this script."
            )
        sys.exit(f"  failed  {name}: {stderr}")
    print(f"  created {name}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Create the UC catalogs required by the bundle before deploy.",
    )
    ap.add_argument("--target", choices=["dev", "prod"], default="dev")
    ap.add_argument("--profile", help="Databricks CLI profile; omit for env-var auth")
    args = ap.parse_args()

    print(f"Bootstrap catalogs for target '{args.target}':")
    for layer in LAYERS:
        ensure_catalog(f"{layer}_{args.target}", args.profile)


if __name__ == "__main__":
    main()
