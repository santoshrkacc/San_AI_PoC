## GenAI_ETL_Code_New — Databricks Data Engineering (Retail: Books & Toys)

End-to-end well-architected ETL on Databricks with bronze→silver→gold layers, a star schema (customers, products, pricing, sales, billing), CI/CD to dev/test/prod, and IaC for Azure Databricks and ADLS.

### Architecture
- Source: Azure Data Lake Storage (ADLS) container (raw data)
- Compute: Azure Databricks (PySpark, Delta Lake)
- Medallion: Bronze (raw curated) → Silver (cleaned) → Gold (star schema)
- Consumption: Power BI, Databricks AI over BI Genie

### Repo Structure
```
conf/                    # env configs (dev/test/prod)
docs/                    # mapping and design docs
notebooks/               # Databricks notebooks (thin, orchestration)
src/                     # reusable PySpark modules
  etl/                   # bronze, silver, gold jobs
  spark/                 # spark session helpers
tests/                   # unit tests (chispa)
terraform/               # IaC for ADLS & Databricks
.github/workflows/       # CI/CD pipelines
```

### Environments
- dev, test, prod with isolated workspaces, storage paths, and secrets.
- Parameters come from `conf/<env>.yaml` and Databricks secret scopes.

### Quickstart (local)
1) `pip install -r requirements.txt`  2) `pre-commit install`
3) `ruff --fix . && mypy . && bandit -r src`
4) `pytest -q`

### Run in Databricks
- Import `notebooks/*.py` or sync repo integration.
- Configure widgets in each notebook: `env`, `bronze_path`, `silver_path`, `gold_path`.
- Attach to a cluster with Runtime 13.x+ and Delta Lake enabled.

### CI/CD
- GitHub Actions deploys notebooks and runs jobs for dev/test/prod using workspace- and env-specific secrets.

### Star Schema (Gold)
- Facts: `fact_sales`
- Dimensions: `dim_customer`, `dim_product`, `dim_pricing`, `dim_billing`, `dim_date`

See `docs/source_to_target_mapping.md` for detailed transformations and DQ rules.

### SCD2
- Use `notebooks/dim_customer_scd2.py`, `notebooks/dim_product_scd2.py`, `notebooks/dim_pricing_scd2.py` and `src/etl/scd2.py` to maintain Type-2 dimensions via Delta MERGE. Upsert facts via `notebooks/fact_sales_merge.py`.

