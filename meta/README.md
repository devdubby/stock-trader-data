# Shared stock metadata

This directory is the canonical data source for stock identity metadata used by the stock recommendation pipelines and apps.

- `stock_master.json`: KRX/KOSPI/KOSDAQ canonical names, codes, markets, optional English display names (`name_en`), and optional classification metadata (`sector`, `themes`).
- `stock_price_stats.json`: mutable price/high cache keyed by stock code. It is a cache for pipeline enrichment; app-facing `price_info` is still emitted into `consensus/latest.json`.
- `us_stock_master.json`: cached US ticker resolutions discovered by the pipeline.

Pipeline code should read these files from `stock-trader-data/meta` first and use app-local `data/meta` files only as a compatibility fallback.
