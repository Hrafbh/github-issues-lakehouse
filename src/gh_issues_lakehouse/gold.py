import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv


def run_gold(config_path: str = "config.yml") -> None:
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    # Load config
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    repos = cfg["source"]["repos"]
    rules = cfg.get("rules", {})

    # Safe defaults
    hours_in_month = rules.get("hours_in_month", cfg.get("hours_in_month", 720))
    sla_by_sev = rules.get("sla_hours_by_severity", {"critical": 24, "major": 72, "minor": 168})

    for r in repos:
        owner = r["owner"]
        repo = r["repo"]

        silver_dir = data_dir / "silver" / f"{owner}__{repo}"
        gold_dir = data_dir / "gold" / f"{owner}__{repo}"
        gold_dir.mkdir(parents=True, exist_ok=True)

        silver_parquet = silver_dir / "issues_silver.parquet"
        silver_csv = silver_dir / "issues_silver.csv"

        if silver_parquet.exists():
            df = pd.read_parquet(silver_parquet)
        elif silver_csv.exists():
            df = pd.read_csv(silver_csv)
        else:
            raise FileNotFoundError(f"Silver file not found in {silver_dir}")

        # Ensure datetime
        for col in ["created_at", "updated_at", "closed_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # We focus on CRITICAL incidents only
        crit = df[(df["severity"] == "critical") & (df["closed_at"].notna())].copy()

        if crit.empty:
            print(f"[gold] No closed critical issues found for {owner}/{repo}.")
            # Still write empty outputs (optional)
            component_out = gold_dir / "kpi_monthly_component.parquet"
            global_out = gold_dir / "kpi_monthly_global.parquet"
            pd.DataFrame().to_parquet(component_out, index=False)
            pd.DataFrame().to_parquet(global_out, index=False)
            continue

        # Month based on closed_at
        crit["month"] = crit["closed_at"].dt.to_period("M").astype(str)

        # Ensure resolution_hours exists (if not, compute)
        if "resolution_hours" not in crit.columns:
            crit["resolution_hours"] = (crit["closed_at"] - crit["created_at"]).dt.total_seconds() / 3600.0

        # SLA target for critical
        crit["sla_hours_target"] = float(sla_by_sev.get("critical", 24))

        # SLA breach (critical only)
        crit["sla_breached"] = crit["resolution_hours"] > crit["sla_hours_target"]

        # --- KPI by component ---
        kpi_comp = (
            crit.groupby(["month", "component"], dropna=False)
            .agg(
                closed_critical=("issue_id", "count"),
                total_critical_hours=("resolution_hours", "sum"),
                avg_resolution_hours=("resolution_hours", "mean"),
                sla_breach_rate=("sla_breached", "mean"),
            )
            .reset_index()
        )

        # Stability index per component (bounded)
        kpi_comp["hours_in_month"] = float(hours_in_month)
        kpi_comp["stability_index"] = 1.0 - (kpi_comp["total_critical_hours"] / kpi_comp["hours_in_month"])
        kpi_comp["stability_index"] = kpi_comp["stability_index"].clip(lower=0.0, upper=1.0)

        # --- Global KPI (monthly) ---
        kpi_global = (
            kpi_comp.groupby("month")
            .agg(
                global_closed_critical=("closed_critical", "sum"),
                global_total_critical_hours=("total_critical_hours", "sum"),
            )
            .reset_index()
        )
        kpi_global["hours_in_month"] = float(hours_in_month)
        kpi_global["global_stability_index"] = 1.0 - (kpi_global["global_total_critical_hours"] / kpi_global["hours_in_month"])
        kpi_global["global_stability_index"] = kpi_global["global_stability_index"].clip(lower=0.0, upper=1.0)

        # Global SLA breach rate (weighted by number of closed critical issues)
        # (more stable than simple mean of component rates)
        tmp = kpi_comp.copy()
        tmp["breaches_estimated"] = tmp["sla_breach_rate"] * tmp["closed_critical"]
        kpi_sla = (
            tmp.groupby("month")
            .agg(
                breaches_estimated=("breaches_estimated", "sum"),
                closed_critical=("closed_critical", "sum"),
            )
            .reset_index()
        )
        kpi_sla["global_sla_breach_rate"] = kpi_sla["breaches_estimated"] / kpi_sla["closed_critical"]
        kpi_global = kpi_global.merge(kpi_sla[["month", "global_sla_breach_rate"]], on="month", how="left")

        # Save outputs (CSV + Parquet)
        comp_parquet = gold_dir / "kpi_monthly_component.parquet"
        comp_csv = gold_dir / "kpi_monthly_component.csv"
        glob_parquet = gold_dir / "kpi_monthly_global.parquet"
        glob_csv = gold_dir / "kpi_monthly_global.csv"

        kpi_comp.to_parquet(comp_parquet, index=False)
        kpi_comp.to_csv(comp_csv, index=False, encoding="utf-8")

        kpi_global.to_parquet(glob_parquet, index=False)
        kpi_global.to_csv(glob_csv, index=False, encoding="utf-8")

        print(f"[gold] saved -> {comp_parquet}")
        print(f"[gold] saved -> {glob_parquet}")

        # Logs utiles
        print(f"[gold] months={kpi_global['month'].nunique()} | total_closed_critical={int(kpi_global['global_closed_critical'].sum())}")
