## Databricks PySpark Engineering Rules

These rules help keep PySpark code in Databricks clear, testable, and production-ready.

### Code Style
- Use Python 3.10+ and type hints everywhere. Prefer `pyspark.sql.types` for schemas.
- Organize notebooks: one orchestration notebook, many importable modules.
- Avoid heavy logic in cells; put logic in functions/classes in `.py` files.
- Prefer DataFrame API over RDDs. Avoid UDFs when SQL functions exist.
- When UDF is necessary, use pandas UDFs with type hints and return types.

### Structure
- `src/` for library code, `notebooks/` for Databricks notebooks, `tests/` for unit tests.
- Use `main()` entry points for jobs; keep side effects behind `if __name__ == "__main__":`.
- Centralize Spark session creation in `src/spark/session.py`.
- Declare schemas explicitly and validate inputs.

### Testing
- Write unit tests for transforms using small in-memory DataFrames.
- Use `chispa` or `assert_spark_frame_equal` style utilities.
- Mock I/O; don’t hit external systems in unit tests.

### Reliability
- Enable `spark.sql.adaptive.enabled=true` and `spark.sql.shuffle.partitions` tuning per environment.
- Use checkpointing for very long lineage chains.
- Cache strategically; unpersist when done.

### Quality Gates
- Run pre-commit on every commit: formatting, lint, types, security.
- No `print()` in production code; use structured logging.
- No broad `except:`. Catch specific exceptions.

### Data Governance
- Use Delta Lake for bronze/silver/gold. Vacuum with retention policy.
- Enforce `expectations` with Deequ or Delta Live Tables when applicable.

### Secrets & Config
- Load configs via environment or Databricks secrets. Do not hardcode.
- Keep env-specific configs in `conf/<env>.yaml`.

### Performance
- Prefer joins with broadcast hints for small dimension tables.
- Avoid `collect()` unless strictly necessary.
- Partition writes by natural partition columns; use Z-Order on high-cardinality filters.

### Notebooks
- Keep notebooks thin: parameter cell, import cell, call orchestration.
- Use `dbutils.widgets` for parameters; read once and pass down.

### Setup
- Install tools: `pip install -r requirements.txt` and `pre-commit install`.
- Run locally: `ruff --fix . && mypy . && bandit -r src`.


