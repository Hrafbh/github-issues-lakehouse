import os
import json
from pathlib import Path
from collections import Counter

import pandas as pd
import yaml
from dotenv import load_dotenv


def extract_component(labels, prefixes):
    """
    labels: list[str]
    prefixes: list[str] like ["module:", "area:"]
    Return a component name (string).
    """
    for lab in labels:
        lab_lower = lab.lower().strip()
        for pref in prefixes:
            pref_lower = pref.lower().strip()
            if lab_lower.startswith(pref_lower):
                # example: "module: io" -> take text after "module:"
                return lab.split(":", 1)[1].strip() if ":" in lab else "other"
    return "other"


def is_critical_issue(labels, critical_labels):
    """
    True if any label is in critical_labels (case-insensitive).
    """
    critical_set = {x.lower().strip() for x in critical_labels}
    for lab in labels:
        if lab.lower().strip() in critical_set:
            return True
    return False


def find_latest_bronze_file(data_dir: Path, owner: str, repo: str) -> Path:
    """
    Example folder: data/bronze/pandas-dev__pandas/issues_YYYYMMDD_HHMMSS.jsonl
    We pick the latest by filename sorting.
    """
    folder = data_dir / "bronze" / f"{owner}__{repo}"
    files = sorted(folder.glob("issues_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No bronze files found in {folder}")
    return files[-1]


def run_silver(config_path: str = "config.yml") -> None:
    # Load .env (DATA_DIR)
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    # Load config.yml
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    repos = cfg["source"]["repos"]

    prefixes = cfg.get("rules", {}).get("component_prefixes", ["module:", "area:", "component:"])
    critical_labels = cfg.get("rules", {}).get("critical_labels", [])

    total_rows = 0

    for r in repos:
        owner = r["owner"]
        repo = r["repo"]

        bronze_file = find_latest_bronze_file(data_dir, owner, repo)

        # Output folder for Silver
        silver_dir = data_dir / "silver" / f"{owner}__{repo}"
        silver_dir.mkdir(parents=True, exist_ok=True)

        out_parquet = silver_dir / "issues.parquet"
        out_csv = silver_dir / "issues.csv"
        out_labels_csv = silver_dir / "label_counts.csv"

        print(f"[silver] input={bronze_file}")
        print(f"[silver] output={out_parquet}")

        rows = []
        label_counter = Counter()

        with open(bronze_file, "r", encoding="utf-8") as f_in:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue

                issue = json.loads(line)

                # Labels list from GitHub payload
                labels = [x.get("name") for x in issue.get("labels", []) if x.get("name")]
                for lab in labels:
                    label_counter[lab] += 1

                comp = extract_component(labels, prefixes)
                crit = is_critical_issue(labels, critical_labels)

                # Keep only essential fields
                rows.append({
                    "issue_id": issue.get("id"),
                    "issue_number": issue.get("number"),
                    "state": issue.get("state"),  # open / closed
                    "title": issue.get("title"),
                    "body": issue.get("body"),
                    "created_at": issue.get("created_at"),
                    "updated_at": issue.get("updated_at"),
                    "closed_at": issue.get("closed_at"),
                    "author_login": (issue.get("user") or {}).get("login"),
                    "labels": "|".join(labels),      # simple string storage
                    "component": comp,
                    "is_critical": crit,
                })

        df = pd.DataFrame(rows)

        # Convert date columns to proper datetime
        for col in ["created_at", "updated_at", "closed_at"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Small summary (open / closed)
        open_count = int((df["state"] == "open").sum())
        closed_count = int((df["state"] == "closed").sum())
        critical_count = int(df["is_critical"].sum())

        print(f"[silver] rows={len(df)} | open={open_count} | closed={closed_count} | critical={critical_count}")

        # Save Parquet + CSV (CSV is easy to open in Excel)
        df.to_parquet(out_parquet, index=False)
        df.to_csv(out_csv, index=False, encoding="utf-8")

        # Save label frequency (to decide critical labels later)
        labels_df = pd.DataFrame(label_counter.most_common(200), columns=["label", "count"])
        labels_df.to_csv(out_labels_csv, index=False, encoding="utf-8")

        total_rows += len(df)

        print(f"[silver] label_counts saved -> {out_labels_csv}")

    print(f"[silver] DONE. total_rows={total_rows}")
