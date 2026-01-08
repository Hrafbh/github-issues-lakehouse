import os
import json
from pathlib import Path
from collections import Counter

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv


def find_latest_bronze_file(data_dir: Path, owner: str, repo: str) -> Path:
    folder = data_dir / "bronze" / f"{owner}__{repo}"
    files = sorted(folder.glob("issues_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No bronze files found in {folder}")
    return files[-1]


def fetch_repo_label_descriptions(owner: str, repo: str, headers: dict, per_page: int = 100) -> dict:
    """Return {label_name: description} from GitHub labels endpoint."""
    page = 1
    out = {}
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/labels"
        r = requests.get(url, headers=headers, params={"per_page": per_page, "page": page}, timeout=60)
        r.raise_for_status()
        items = r.json()
        if not items:
            break
        for lb in items:
            name = lb.get("name")
            if name:
                out[name] = lb.get("description") or ""
        if len(items) < per_page:
            break
        page += 1
    return out


def compute_issue_type(labels: list[str], type_labels: dict) -> str:
    if any(l in labels for l in type_labels.get("bug", [])):
        return "bug"
    if any(l in labels for l in type_labels.get("docs", [])):
        return "docs"
    if any(l in labels for l in type_labels.get("enhancement", [])):
        return "enhancement"
    if any(l in labels for l in type_labels.get("question", [])):
        return "question"
    return "other"


def compute_severity(labels: list[str], issue_type: str, severity_cfg: dict, type_labels: dict) -> str:
    crit = set(severity_cfg.get("critical", []))
    maj = set(severity_cfg.get("major", []))

    if any(l in labels for l in crit):
        return "critical"
    if any(l in labels for l in maj):
        return "major"

    # Minor fallback only if it's a bug (optional)
    if severity_cfg.get("minor_fallback_if_bug", True):
        bug_labels = set(type_labels.get("bug", []))
        if issue_type == "bug" or any(l in labels for l in bug_labels):
            return "minor"

    return "na"


def pick_component(labels: list[str], cfg: dict, meta_labels: set[str]) -> str:
    prefixes = [p.lower().strip() for p in cfg.get("prefixes", [])]
    allowlist = set(cfg.get("allowlist", []))

    # 1) prefix rule: "module: io" -> "io"
    for lab in labels:
        lab_low = lab.lower().strip()
        for pref in prefixes:
            if lab_low.startswith(pref):
                return lab.split(":", 1)[1].strip() if ":" in lab else "other"

    # 2) allowlist rule
    for lab in labels:
        if lab in allowlist:
            return lab

    # 3) fallback: first label that is not meta (not type/process/severity)
    for lab in labels:
        if lab not in meta_labels:
            return lab

    return "other"


def run_silver(config_path: str = "config.yml") -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    rules = cfg.get("rules", {})

    type_labels = rules.get("type_labels", {})
    process_labels = set(rules.get("process_labels", []))
    severity_labels = rules.get("severity_labels", {})
    component_cfg = rules.get("component", {})
    sla_by_sev = rules.get("sla_hours_by_severity", {"critical": 24, "major": 72, "minor": 168})

    # Build meta_labels set (labels that should NOT become "component")
    meta_labels = set()
    for k, v in type_labels.items():
        meta_labels.update(v)
    meta_labels.update(process_labels)
    meta_labels.update(severity_labels.get("critical", []))
    meta_labels.update(severity_labels.get("major", []))

    headers = {"Accept": "application/vnd.github+json", "User-Agent": "gh-issues-lakehouse"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for r in cfg["source"]["repos"]:
        owner, repo = r["owner"], r["repo"]
        bronze_file = find_latest_bronze_file(data_dir, owner, repo)

        silver_dir = data_dir / "silver" / f"{owner}__{repo}"
        silver_dir.mkdir(parents=True, exist_ok=True)

        out_csv = silver_dir / "issues_silver.csv"
        out_parquet = silver_dir / "issues_silver.parquet"
        out_label_csv = silver_dir / "label_catalog.csv"
        out_label_parquet = silver_dir / "label_catalog.parquet"

        print(f"[silver] bronze={bronze_file}")

        rows = []
        label_counter = Counter()

        # --- Read bronze JSONL ---
        with open(bronze_file, "r", encoding="utf-8") as f_in:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                issue = json.loads(line)

                labels = [x.get("name") for x in issue.get("labels", []) if x.get("name")]
                for lab in labels:
                    label_counter[lab] += 1

                issue_type = compute_issue_type(labels, type_labels)
                severity = compute_severity(labels, issue_type, severity_labels, type_labels)
                component = pick_component(labels, component_cfg, meta_labels)

                rows.append({
                    "issue_id": issue.get("id"),
                    "issue_number": issue.get("number"),
                    "state": issue.get("state"),
                    "title": issue.get("title"),
                    "body": issue.get("body"),
                    "created_at": issue.get("created_at"),
                    "updated_at": issue.get("updated_at"),
                    "closed_at": issue.get("closed_at"),
                    "labels": "|".join(labels),
                    "issue_type": issue_type,
                    "severity": severity,
                    "component": component,
                    "incident_like": (severity in ("critical", "major", "minor")),
                })

        df = pd.DataFrame(rows)

        # Parse dates
        for col in ["created_at", "updated_at", "closed_at"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Resolution time (only if closed)
        df["resolution_hours"] = (df["closed_at"] - df["created_at"]).dt.total_seconds() / 3600.0
        df.loc[df["closed_at"].isna(), "resolution_hours"] = pd.NA

        # SLA target + breach
        df["sla_hours_target"] = df["severity"].map(sla_by_sev).fillna(pd.NA)

        def breached(row):
            if row["severity"] not in ("critical", "major", "minor"):
                return False
            if pd.isna(row["resolution_hours"]) or pd.isna(row["sla_hours_target"]):
                return False
            return float(row["resolution_hours"]) > float(row["sla_hours_target"])

        df["sla_breached"] = df.apply(breached, axis=1)

        # Save BOTH CSV and Parquet
        df.to_csv(out_csv, index=False, encoding="utf-8")
        df.to_parquet(out_parquet, index=False)

        print(f"[silver] saved -> {out_csv}")
        print(f"[silver] saved -> {out_parquet}")

        # --- Label catalog (count + description) ---
        desc_map = fetch_repo_label_descriptions(owner, repo, headers=headers, per_page=100)

        labels_df = pd.DataFrame(label_counter.most_common(), columns=["label", "count"])
        labels_df["description"] = labels_df["label"].map(desc_map).fillna("")

        # Basic label category
        def category(label_name: str) -> str:
            if label_name in severity_labels.get("critical", []):
                return "severity_critical"
            if label_name in severity_labels.get("major", []):
                return "severity_major"
            if label_name in process_labels:
                return "process"
            if label_name in meta_labels:
                return "type"
            if label_name in set(component_cfg.get("allowlist", [])):
                return "component"
            return "other"

        labels_df["category"] = labels_df["label"].apply(category)

        labels_df.to_csv(out_label_csv, index=False, encoding="utf-8")
        labels_df.to_parquet(out_label_parquet, index=False)

        # Logs utiles
        open_count = int((df["state"] == "open").sum())
        closed_count = int((df["state"] == "closed").sum())
        sev_counts = df["severity"].value_counts(dropna=False).to_dict()
        print(f"[silver] stats: open={open_count} closed={closed_count}")
        print(f"[silver] severity_counts={sev_counts}")

        pivot = df.pivot_table(index="severity", columns="state", values="issue_id", aggfunc="count", fill_value=0)
        print("[silver] open/closed by severity:\n", pivot)
