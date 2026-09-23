# oc

personal analytics and recorder operations workspace. shared library and production code live in the sibling `umm` repository.

## where things live

- `analytics/notebooks/`: current local studies; start with `playground.ipynb` for api examples or `jdbp_cap.ipynb` for capacity analysis. notebooks remain local until credentials and saved outputs are reviewed.
- `analytics/scripts/`: markout/comparison entry points and shared output paths.
- `analytics/scripts/audits/`: recording coverage, identity and alignment checks.
- `analytics/scripts/market_data/`: latency and migration validation tools.
- `analytics/src/analytics/`: older notebook helpers; credential-bearing loaders are excluded from git.
- `analytics/tests/`: offline analytics checks.
- `operations/direct_recorders/`: recorder checks, migration tools, runbooks and dated reports.
- `operations/influx_archive/`: backup, health and prediction-series maintenance tools.
- `operations/migrate/`: historical data migration/repair tools and their tests.
- `operations/utilities/`: bitstamp capture/audit, fix timing and connection-test tools.
- `archive/cleanup_20260922/`: preserved older notebooks, obsolete pnl diagnostics and saved bitstamp captures; local only, with a move manifest.

## local setup

keep this directory alongside `umm` under the jst workspace. use the existing umm python environment and each tool's runbook. generated analytics output is controlled by `JST_ANALYTICS_DIR`; see `analytics/scripts/analytics_paths.py`.

operational tools retain their existing paths. archiving is not a claim that a historical migration has completed; review the tool's documented plan/apply behavior before running it.

## private repository preparation

this directory has not been initialized or pushed. `.gitignore` keeps local archives, notebooks pending review, credential-bearing legacy helpers, caches and raw captures out of the initial commit. private visibility does not make embedded credentials appropriate to commit.

review staged files before the first commit. the initial repository can contain the maintained scripts, tests and runbooks; notebooks can be added after a separate credential/output cleanup.
