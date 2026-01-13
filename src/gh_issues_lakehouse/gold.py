import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv


def _ensure_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    return df


def _month_str(dt_series: pd.Series) -> pd.Series:
    # "2025-01" format
    return dt_series.dt.to_period("M").astype(str)


def run_gold(config_path: str = "config.yml") -> None:
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    repos = cfg["source"]["repos"]
    rules = cfg.get("rules", {})

    # Buckets (stats de temps)
    buckets = rules.get("time_buckets_hours", [24, 72, 168, 336])

    for r in repos:
        owner, repo = r["owner"], r["repo"]

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
            raise FileNotFoundError(f"No silver file found in {silver_dir}")

        df = _ensure_datetime(df, ["created_at", "closed_at", "updated_at"])

        # Focus: defect-like tickets only (adapté GitHub)
        df_def = df[df["ticket_kind"] == "defect"].copy()

        if df_def.empty:
            print(f"[gold] No defect tickets found for {owner}/{repo}.")
            continue

        # --- CREATED (flow in) ---
        created = df_def.copy()
        created = created[created["created_at"].notna()]
        created["month"] = _month_str(created["created_at"])

        created_kpi = (
            created.groupby(["month", "component", "priority_tier"], dropna=False)
            .agg(created_count=("issue_id", "count"))
            .reset_index()
        )

        # --- CLOSED (flow out + resolution stats) ---
        closed = df_def.copy()
        closed = closed[closed["closed_at"].notna() & closed["created_at"].notna()]
        closed["month"] = _month_str(closed["closed_at"])

        # resolution_hours: si déjà calculé en Silver, on le garde ; sinon on le calcule
        if "resolution_hours" not in closed.columns or closed["resolution_hours"].isna().all():
            closed["resolution_hours"] = (closed["closed_at"] - closed["created_at"]).dt.total_seconds() / 3600.0
        else:
            # au cas où c'est string
            closed["resolution_hours"] = pd.to_numeric(closed["resolution_hours"], errors="coerce")

        def p90(x):
            return x.quantile(0.90)

        closed_kpi = (
            closed.groupby(["month", "component", "priority_tier"], dropna=False)
            .agg(
                closed_count=("issue_id", "count"),
                avg_resolution_hours=("resolution_hours", "mean"),
                median_resolution_hours=("resolution_hours", "median"),
                p90_resolution_hours=("resolution_hours", p90),
            )
            .reset_index()
        )

        # Buckets: share closed within X hours
        for b in buckets:
            col = f"share_closed_within_{int(b)}h"
            tmp = closed.copy()
            tmp[col] = (tmp["resolution_hours"] <= float(b))
            bucket_kpi = (
                tmp.groupby(["month", "component", "priority_tier"], dropna=False)[col]
                .mean()
                .reset_index()
            )
            closed_kpi = closed_kpi.merge(bucket_kpi, on=["month", "component", "priority_tier"], how="left")

        # --- MERGE created + closed into one monthly table ---
        kpi = created_kpi.merge(closed_kpi, on=["month", "component", "priority_tier"], how="outer")

        # Fill counts with 0
        kpi["created_count"] = kpi["created_count"].fillna(0).astype(int)
        kpi["closed_count"] = kpi["closed_count"].fillna(0).astype(int)

        # --- BACKLOG END (cumulative created - cumulative closed) ---
        # We compute backlog per (component, priority_tier) across months
        kpi = kpi.sort_values(["component", "priority_tier", "month"])

        kpi["backlog_end"] = (
            kpi.groupby(["component", "priority_tier"], dropna=False)["created_count"].cumsum()
            - kpi.groupby(["component", "priority_tier"], dropna=False)["closed_count"].cumsum()
        )

        # --- GLOBAL monthly KPI (all components, weighted) ---
        global_kpi = kpi.groupby("month", dropna=False).agg(
            created_count=("created_count", "sum"),
            closed_count=("closed_count", "sum"),
            backlog_end=("backlog_end", "sum"),
        ).reset_index()

        # Weighted averages for resolution metrics (weighted by closed_count)
        # We compute them from closed_kpi, because those metrics exist only when closed_count > 0
        ck = closed_kpi.copy()
        ck["w"] = ck["closed_count"]

        def weighted_avg(df_, col):
            num = (df_[col] * df_["w"]).sum()
            den = df_["w"].sum()
            return num / den if den else None

        global_res = ck.groupby("month").apply(
            lambda g: pd.Series({
                "avg_resolution_hours": weighted_avg(g, "avg_resolution_hours"),
                "median_resolution_hours": weighted_avg(g, "median_resolution_hours"),
                "p90_resolution_hours": weighted_avg(g, "p90_resolution_hours"),
                **{f"share_closed_within_{int(b)}h": weighted_avg(g, f"share_closed_within_{int(b)}h") for b in buckets},
            })
        ).reset_index()

        global_kpi = global_kpi.merge(global_res, on="month", how="left")

        # --- SAVE outputs (CSV + Parquet) ---
        out_comp_csv = gold_dir / "kpi_monthly_component_tier.csv"
        out_comp_parquet = gold_dir / "kpi_monthly_component_tier.parquet"
        out_global_csv = gold_dir / "kpi_monthly_global.csv"
        out_global_parquet = gold_dir / "kpi_monthly_global.parquet"

        kpi.to_csv(out_comp_csv, index=False, encoding="utf-8")
        kpi.to_parquet(out_comp_parquet, index=False)
        global_kpi.to_csv(out_global_csv, index=False, encoding="utf-8")
        global_kpi.to_parquet(out_global_parquet, index=False)

        print(f"[gold] saved -> {out_comp_parquet}")
        print(f"[gold] saved -> {out_global_parquet}")
        print(f"[gold] months={global_kpi['month'].nunique()} | created={int(global_kpi['created_count'].sum())} | closed={int(global_kpi['closed_count'].sum())}")
