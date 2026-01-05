# github-issues-lakehouse (pandas)

A small **data engineering** project that builds a Bronze/Silver/Gold lakehouse from **GitHub Issues**
of the `pandas-dev/pandas` repository.

The goal is to transform raw issue data into monthly KPI tables that describe **operational stability**
and **responsiveness** for critical issues.

## Data source
- Repository: `pandas-dev/pandas`
- Endpoint: GitHub Issues API
- Note: the Issues endpoint can also return pull requests; we exclude items that contain the `pull_request` field.

## Storage layers
### Bronze (raw)
- Raw API payloads saved as **JSON Lines** (`.jsonl`)
- One extraction run produces one file under `data/bronze/...`

### Silver (clean)
- Cleaned and normalized tables saved as **Parquet**
- Stable schema (typed timestamps, normalized labels, etc.)
- Deduplication to avoid duplicates across runs

### Gold (KPI marts)
- Monthly KPI tables saved as **Parquet**
- Ready for dashboards or further analysis

## Business rules (configurable)
All rules are defined in `config.yml`.

### 1) Component rule (replacement for "scope")
Each issue is assigned to a **component** using labels:
- We search for the first label starting with one of these prefixes:
  - `component:`
  - `module:`
  - `area:`
- If no such label exists, `component = "other"`

### 2) Critical issue rule
This project focuses on **critical issues only**.
An issue is considered critical if it has at least one label listed in:
- `rules.critical_labels`

### 3) SLA rule (critical only)
We define a simple SLA for **resolution time**:
- If `resolution_hours > rules.sla_hours_critical` → SLA breach

### 4) Monthly time base
For monthly indices, we use:
- `rules.hours_in_month` (default: 720)

## Monthly KPIs (per component)
For each month and each component:
- **total_critical_hours**: total hours critical issues stayed open (based on created_at/closed_at)
- **stability_index**: `1 - (total_critical_hours / hours_in_month)`
- **sla_breach_rate**: % of critical issues that exceeded the SLA threshold
- **created_critical**: number of critical issues created in the month
- **closed_critical**: number of critical issues closed in the month
- **backlog_critical_end**: number of open critical issues at end of month (optional)

## How to run (local)
1) Create a virtual environment and install dependencies
2) Configure `.env` and `config.yml`
3) Run the pipeline commands (ingest → silver → gold)

## Outputs
- `data/bronze/.../*.jsonl`
- `data/silver/*.parquet`
- `data/gold/kpi_monthly_*.parquet`

## Notes
- This repository contains **code and rules**, not private datasets.
- Raw data is re-downloadable from GitHub; do not commit `data/`.
