# Shared stock metadata

This directory is the canonical data source for stock identity metadata used by the stock recommendation pipelines and apps.

- `stock_master.json`: KRX/KOSPI/KOSDAQ canonical names, codes, markets, and optional English display names (`name_en`).
- `us_stock_master.json`: cached US ticker resolutions discovered by the pipeline.

Pipeline code should read these files from `stock-trader-data/meta` first and use app-local `data/meta` files only as a compatibility fallback.
