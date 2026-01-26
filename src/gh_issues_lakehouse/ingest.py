import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests
import yaml
from dotenv import load_dotenv


def run_ingest(config_path: str = "config.yml") -> None:
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN", "").strip()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    repos = cfg["source"]["repos"]

    per_page = cfg.get("ingestion", {}).get("per_page", 100)
    since = cfg.get("ingestion", {}).get("full_since")  # filtre sur updated_at (pas created_at) :contentReference[oaicite:1]{index=1}

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gh-issues-lakehouse",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    total_saved = 0

    session = requests.Session()

    for r in repos:
        owner = r["owner"]
        repo = r["repo"]

        out_dir = data_dir / "bronze" / f"{owner}__{repo}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"issues_{run_id}.jsonl"

        print(f"[ingest] repo={owner}/{repo} -> {out_file}")

        saved_for_repo = 0
        base_url = f"https://api.github.com/repos/{owner}/{repo}/issues"

        # 1ère requête: params “classiques”
        params = {"state": "all", "per_page": per_page}
        if since:
            params["since"] = since

        next_url = base_url
        first = True

        with open(out_file, "w", encoding="utf-8") as f_out:
            while next_url:
                if first:
                    resp = session.get(next_url, headers=headers, params=params, timeout=60)
                    first = False
                else:
                    # next_url contient déjà le curseur (after/before) dans l’URL
                    resp = session.get(next_url, headers=headers, timeout=60)

                if resp.status_code == 422:
                    print("[ingest] 422 body:", resp.text)
                    break

                resp.raise_for_status()
                items = resp.json()
                if not items:
                    break

                # Issues endpoint peut inclure des PRs → on les exclut :contentReference[oaicite:2]{index=2}
                issues_only = [it for it in items if "pull_request" not in it]

                for it in issues_only:
                    f_out.write(json.dumps(it, ensure_ascii=False) + "\n")

                saved_for_repo += len(issues_only)

                # Cursor-based pagination: on suit le header Link (rel="next")
                next_url = resp.links.get("next", {}).get("url")

        print(f"[ingest] saved={saved_for_repo} issues (PRs excluded)")
        total_saved += saved_for_repo

    print(f"[ingest] DONE. total_saved={total_saved}")
