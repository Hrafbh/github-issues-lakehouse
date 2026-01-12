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


def compute_ticket_kind(labels: list[str], kind_labels: dict) -> str:
    if any(l in labels for l in kind_labels.get("bug", [])):
        return "defect"
    if any(l in labels for l in kind_labels.get("docs", [])):
        return "docs"
    if any(l in labels for l in kind_labels.get("enhancement", [])):
        return "enhancement"
    if any(l in labels for l in kind_labels.get("question", [])):
        return "question"
    return "other"


def compute_priority_tier(labels: list[str], ticket_kind: str, priority_cfg: dict, kind_labels: dict) -> str:
    # Only meaningful for defect-like tickets
    if ticket_kind != "defect":
        # But still allow explicit P0/P1 even if "Bug" label missing (rare)
        if any(l in labels for l in priority_cfg.get("P0", [])):
            return "P0"
        if any(l in labels for l in priority_cfg.get("P1", [])):
            return "P1"
        return "NA"

    if any(l in labels for l in priority_cfg.get("P0", [])):
        return "P0"
    if any(l in labels for l in priority_cfg.get("P1", [])):
        return "P1"

    # Fallback: if Bug and not P0/P1 -> P2
    if priority_cfg.get("P2_fallback_if_bug", True):
        bug_labels = set(kind_labels.get("bug", []))
        if any(l in labels for l in bug_labels):
            return "P2"

    return "NA"


def pick_component(labels: list[str], component_cfg: dict, meta_labels: set[str]) -> str:
    prefixes = [p.lower().strip() for p in component_cfg.get("prefixes", [])]
    allowlist = set(component_cfg.get("allowlist", []))

    # 1) prefix rule (if exists)
    for lab in labels:
        lab_low = lab.lower().strip()
        for pref in prefixes:
            if lab_low.startswith(pref):
                return lab.split(":", 1)[1].strip() if ":" in lab else "other"

    # 2) allowlist rule (common for pandas)
    for lab in labels:
        if lab in allowlist:
            return lab

    # 3) fallback: first non-meta label
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
    kind_labels = rules.get("kind_labels", {})
    process_labels = set(rules.get("process_labels", []))
    priority_cfg = rules.get("priority_tiers", {})
    component_cfg = rules.get("component", {})

    # Labels that should NOT become components
    meta_labels = set(process_labels)
    for _, v in kind_labels.items():
        meta_labels.update(v)
    meta_labels.update(priority_cfg.get("P0", []))
    meta_labels.update(priority_cfg.get("P1", []))

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
        counter = Counter()

        with open(bronze_file, "r", encoding="utf-8") as f_in:
            for line in f_in:
                line = line.strip()
                if not line:
                    continue
                issue = json.loads(line)

                labels = [x.get("name") for x in issue.get("labels", []) if x.get("name")]
                for lab in labels:
                    counter[lab] += 1

                ticket_kind = compute_ticket_kind(labels, kind_labels)
                priority_tier = compute_priority_tier(labels, ticket_kind, priority_cfg, kind_labels)
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
                    "ticket_kind": ticket_kind,
                    "priority_tier": priority_tier,
                    "component": component,
                })

        df = pd.DataFrame(rows)

        for col in ["created_at", "updated_at", "closed_at"]:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        # Resolution hours for closed issues
        df["resolution_hours"] = (df["closed_at"] - df["created_at"]).dt.total_seconds() / 3600.0
        df.loc[df["closed_at"].isna(), "resolution_hours"] = pd.NA

        # Save
        df.to_csv(out_csv, index=False, encoding="utf-8")
        df.to_parquet(out_parquet, index=False)

        print(f"[silver] saved -> {out_csv}")
        print(f"[silver] saved -> {out_parquet}")
        print(f"[silver] stats open={(df['state']=='open').sum()} closed={(df['state']=='closed').sum()}")
        print(f"[silver] tier_counts={df['priority_tier'].value_counts(dropna=False).to_dict()}")

        # Label catalog (count + description)
        desc_map = fetch_repo_label_descriptions(owner, repo, headers=headers, per_page=100)
        labels_df = pd.DataFrame(counter.most_common(), columns=["label", "count"])
        labels_df["description"] = labels_df["label"].map(desc_map).fillna("")
        labels_df.to_csv(out_label_csv, index=False, encoding="utf-8")
        labels_df.to_parquet(out_label_parquet, index=False)
        print(f"[silver] label_catalog saved -> {out_label_csv}")
