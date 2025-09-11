## dbt Checks Pack

Runs `dbt test` and aggregates results from `target/run_results.json`.

### Config (pack_conf.json)
- `project_dir`: dbt project directory (default `.`)
- `profiles_dir`: path to dbt profiles (default `~/.dbt`)
- `target`: dbt target (optional)
- `models`: model/test selection (optional)
- `threads`: parallelism (default 4)
- `vars`: dbt variables dictionary (optional)

### Metrics
- `tests_total`, `tests_passed`, `tests_failed`
- `score` = ratio of passed tests
