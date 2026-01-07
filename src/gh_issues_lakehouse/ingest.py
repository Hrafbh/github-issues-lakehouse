import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests
import yaml
from dotenv import load_dotenv


def run_ingest(config_path: str = "config.yml") -> None:
    """
    Download GitHub issues for the repos listed in config.yml,
    and save raw results to Bronze as JSON Lines (.jsonl).

    Bronze = raw, no transformation, just storage.
    """

    # 1) Load environment variables from .env (token + data dir)
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    # 2) Load config.yml
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    repos = cfg["source"]["repos"]  # list of {owner, repo}

    # Ingestion settings (with safe defaults)
    per_page = cfg.get("ingestion", {}).get("per_page", 100)
    since = cfg.get("ingestion", {}).get("full_since")  # optional ISO string

    # 3) Prepare headers for GitHub API
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gh-issues-lakehouse",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # 4) Run id: used in output filename/folder
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    total_saved = 0

    for r in repos:
        owner = r["owner"]
        repo = r["repo"]

        # Output path: data/bronze/<owner>__<repo>/issues_<run_id>.jsonl
        out_dir = data_dir / "bronze" / f"{owner}__{repo}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"issues_{run_id}.jsonl"

        print(f"[ingest] repo={owner}/{repo} -> {out_file}")

        page = 1
        saved_for_repo = 0

        with open(out_file, "w", encoding="utf-8") as f_out:
            while True:
                url = f"https://api.github.com/repos/{owner}/{repo}/issues"
                params = {
                    "state": "all",
                    "per_page": per_page,
                    "page": page,
                }
                # since filters by updated time
                if since:
                    params["since"] = since

                resp = requests.get(url, headers=headers, params=params, timeout=60)
                resp.raise_for_status()

                items = resp.json()
                if not items:
                    break

                # IMPORTANT: Issues API can return pull requests.
                # We keep only real issues (no "pull_request" key).
                issues_only = [it for it in items if "pull_request" not in it]

                for it in issues_only:
                    f_out.write(json.dumps(it, ensure_ascii=False) + "\n")

                saved_for_repo += len(issues_only)

                # Simple pagination stop rule:
                # If the API returned less than a full page, we reached the end.
                if len(items) < per_page:
                    break

                page += 1

        print(f"[ingest] saved={saved_for_repo} issues (PRs excluded)")
        total_saved += saved_for_repo

    print(f"[ingest] DONE. total_saved={total_saved}")
