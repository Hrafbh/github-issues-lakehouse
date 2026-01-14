import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

def run_demo():
    load_dotenv()
    data_dir = Path(os.getenv("DATA_DIR", "./data")).resolve()

    path = data_dir / "gold" / "pandas-dev__pandas" / "kpi_monthly_global.csv"
    if not path.exists():
        raise FileNotFoundError("Gold output not found. Run: python -m gh_issues_lakehouse gold")

    df = pd.read_csv(path)
    print("[demo] Showing last 5 months:")
    print(df.tail(5).to_string(index=False))
