import os
import json
from pathlib import Path
from collections import Counter

import requests
import pandas as pd
import yaml
from dotenv import load_dotenv


def find_latest_bronze_file(data_dir: Path, owner: str, repo: str) -> Path:
    folder = data_dir / "bronze" / f"{owner}__{repo}"
    files = sorted(folder.glob("issues_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No bronze files found in {folder}")
    return files[-1]


def fetch_repo_label_descriptions(owner: str, repo: str, headers: dict, per_page: int = 100) -> dict:
    # Returns {label_name: description}
    page = 1
    desc_map = {}

    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/labels"
        params = {"per_page": per_page, "page": page}

        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        items = r.json()
        if not items:
            break

        for lb in items:
            name = lb.get("name")
            if name:
                desc_map[name] = lb.get("description") or ""

        if len(items) < per_page:
            break
        page += 1

    return desc_map


def pick_component(labels, component_cfg):
    # 1) try prefixes like "module: io"
    prefixes = [p.lower().strip() for p in component_cfg.get("prefixes", [])]
    for lab in labels:
        lab_lower = lab.lower().strip()
        for pref in prefixes:
            if lab_lower.startswith(pref):
                # take text after first ":" if exists
                return lab.split(":", 1)[1].strip() if ":" in lab else "other"

    # 2) try allowlist labels like "Indexing", "Groupby", ...
    allowlist = component_cfg.get("allowlist", [])
    label_set = set(labels)
    for comp in allowlist:
        if comp in label_set:
            return comp

    return "other"


def compute_issue_type(labels, label_sets):
    # Priority order (simple and deterministic)
    if any(l in labels for l in label_sets.get("bug", [])):
        return "bug"
    if any(l in labels for l in label_sets.get("docs", [])):
        return "docs"
    if any(l in labels for l in label_sets.get("enhancement", [])):
        return "enhancement"
    if any(l in labels for l in label_sets.get("question", [])):
        return "question"
    return "other"


def compute_severity(labels, label_sets):
    # Severity is meaningful mainly for bug-like issues
    if any(l in labels for l in label_sets.get("severity_critical", [])):
        return "critical"
    if any(l in labels for l in label_sets.get("severity_major", [])):
        return "major"
    if any(l in labels for l in label_sets.get("bug", [])):
        return "minor"
    return "na"


def label_family(label_name, label_sets, component_allowlist):
    # Used for the label catalog output
    if label_name in label_sets.get("severity_critical", []):
        return "severity_critical"
    if label_name in label_sets.get("severity_major", []):
        return "severity_major"
    if label_name in label_sets.get("bug", []) or label_name in label_sets.get("enhancement", []) or label_name in label_sets.get("docs", []) or label_name in label_sets.get("question", []):
        return "type"
    if label_name in label_sets.get("process", []):
        return "process"
    if label_name in component_allowlist:
        return "component"
    return "other"


def run_silver(config_path: str = "config.yml") -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    rules = cfg.get("rules", {})
    label_sets = rules.get("label_sets", {})
    component_cfg = rules.get("component", {})
    component_allowlist = component_cfg.get("allowlist", [])

    sla_by_sev = rules.get("sla_hours_by_severity", {"critical": 24, "major": 72, "minor": 168})

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gh-issues-lakehouse",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for r in cfg["source"]["repos"]:
        owner = r["owner"]
        repo = r["repo"]

        bronze_file = find_latest_bronze_file(data_dir, owner, repo)

        silver_dir = data_dir / "silver" / f"{owner}__{repo}"
        silver_dir.mkdir(parents=True, exist_ok=True)

        issues_out = silver_dir / "issues_silver.csv"
        labels_out = silver_dir / "label_catalog.csv"

        print(f"[silver] bronze={bronze_file}")

        rows = []
        counter = Counter()

        # Read bronze JSONL
        with open(bronze_file, "r", encoding="utf-8") as f_in:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                issue = json.loads(line)

                labels = [x.get("name") for x in issue.get("labels", []) if x.get("name")]
                for lab in labels:
                    counter[lab] += 1

                itype = compute_issue_type(labels, label_sets)
                sev = compute_severity(labels, label_sets)
                comp = pick_component(labels, component_cfg)

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
                    "issue_type": itype,
                    "severity": sev,
                    "is_critical": (sev == "critical"),
                    "component": comp,
                })

        df = pd.DataFrame(rows)

        # Convert timestamps
        for col in ["created_at", "updated_at", "closed_at"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Resolution time + SLA breach (only if closed)
        df["resolution_hours"] = (df["closed_at"] - df["created_at"]).dt.total_seconds() / 3600.0
        df.loc[df["closed_at"].isna(), "resolution_hours"] = pd.NA

        def sla_breached(row):
            if row["severity"] not in ("critical", "major", "minor"):
                return False
            if pd.isna(row["resolution_hours"]):
                return False
            return float(row["resolution_hours"]) > float(sla_by_sev.get(row["severity"], 999999))

        df["sla_breached"] = df.apply(sla_breached, axis=1)

        # Save silver issues (CSV is easy to open / visualize)
        df.to_csv(issues_out, index=False, encoding="utf-8")
        print(f"[silver] issues saved -> {issues_out} (rows={len(df)})")

        # Build label catalog (count + description + family)
        desc_map = fetch_repo_label_descriptions(owner, repo, headers=headers, per_page=100)
        labels_df = pd.DataFrame(counter.most_common(), columns=["label", "count"])
        labels_df["description"] = labels_df["label"].map(desc_map).fillna("")
        labels_df["family"] = labels_df["label"].apply(lambda x: label_family(x, label_sets, component_allowlist))
        labels_df.to_csv(labels_out, index=False, encoding="utf-8")
        print(f"[silver] label catalog saved -> {labels_out} (labels={len(labels_df)})")

        # Logs: open/closed + severity distribution
        open_count = int((df["state"] == "open").sum())
        closed_count = int((df["state"] == "closed").sum())
        print(f"[silver] stats: open={open_count} closed={closed_count}")

        sev_counts = df["severity"].value_counts(dropna=False).to_dict()
        print(f"[silver] severity_counts={sev_counts}")


