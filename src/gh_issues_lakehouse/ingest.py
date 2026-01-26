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

    IMPORTANT:
    - GitHub may refuse page-based pagination for large datasets (page=...).
      So we use cursor-based pagination by following the Link header (resp.links["next"]["url"]).
    - Optional filters:
        * ingestion.full_since -> filters by UPDATED time (GitHub "since" parameter)
        * source.created_from -> local filter by CREATED time (created_at)
    """

    # 1) Load environment variables from .env (token + data dir)
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    # 2) Load config.yml
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Repos list
    repos = cfg.get("source", {}).get("repos", [])
    if not repos:
        raise ValueError("config.yml missing source.repos (list of {owner, repo})")

    # Optional: filter by created_at (LOCAL filter)
    created_from = cfg.get("source", {}).get("created_from")  # "YYYY-MM-DD"
    created_from_dt = None
    if created_from:
        # interpret as UTC midnight
        created_from_dt = datetime.fromisoformat(created_from).replace(tzinfo=timezone.utc)

    # Ingestion settings
    per_page = int(cfg.get("ingestion", {}).get("per_page", 100))
    since = cfg.get("ingestion", {}).get("full_since")  # optional ISO string (UPDATED time)

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
    session = requests.Session()

    for r in repos:
        owner = r["owner"]
        repo = r["repo"]

        # Output path: data/bronze/<owner>__<repo>/issues_<run_id>.jsonl
        out_dir = data_dir / "bronze" / f"{owner}__{repo}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"issues_{run_id}.jsonl"

        print(f"[ingest] repo={owner}/{repo} -> {out_file}")
        if created_from_dt:
            print(f"[ingest] created_from={created_from_dt.date().isoformat()} (UTC)")
        if since:
            print(f"[ingest] full_since(updated)={since}")

        saved_for_repo = 0

        base_url = f"https://api.github.com/repos/{owner}/{repo}/issues"

        # First request uses params
        params = {"state": "all", "per_page": per_page}
        if since:
            params["since"] = since  # GitHub filters by UPDATED time

        next_url = base_url
        first = True

        with open(out_file, "w", encoding="utf-8") as f_out:
            while next_url:
                if first:
                    resp = session.get(next_url, headers=headers, params=params, timeout=60)
                    first = False
                else:
                    # next_url already contains cursor info in the URL
                    resp = session.get(next_url, headers=headers, timeout=60)

                if resp.status_code == 422:
                    # Show the message and stop cleanly
                    print("[ingest] 422 body:", resp.text)
                    break

                resp.raise_for_status()
                items = resp.json()
                if not items:
                    break

                # IMPORTANT: Issues endpoint can return pull requests -> exclude them
                issues_only = [it for it in items if "pull_request" not in it]

                for it in issues_only:
                    # Optional local filter by CREATED time
                    if created_from_dt:
                        created_at = datetime.fromisoformat(it["created_at"].replace("Z", "+00:00"))
                        if created_at < created_from_dt:
                            continue

                    f_out.write(json.dumps(it, ensure_ascii=False) + "\n")
                    saved_for_repo += 1

                # Cursor-based pagination: follow Link header "next"
                next_url = resp.links.get("next", {}).get("url")

        print(f"[ingest] saved={saved_for_repo} issues (PRs excluded)")
        total_saved += saved_for_repo

    print(f"[ingest] DONE. total_saved={total_saved}")
