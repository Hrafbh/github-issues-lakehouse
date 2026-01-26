from pathlib import Path
import os
import shutil

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", str(ROOT / "data"))).resolve()
DOCS_DATA = ROOT / "docs" / "data"

OWNER_REPO = "pandas-dev__pandas"  # adapte si besoin

def main():
    DOCS_DATA.mkdir(parents=True, exist_ok=True)

    src_global = DATA_DIR / "gold" / OWNER_REPO / "kpi_monthly_global.csv"
    if not src_global.exists():
        raise FileNotFoundError(f"Missing: {src_global} (run gold first)")

    shutil.copy2(src_global, DOCS_DATA / "kpi_monthly_global.csv")
    print("OK: exported docs/data/kpi_monthly_global.csv")

if __name__ == "__main__":
    main()
