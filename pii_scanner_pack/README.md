## PII Scanner Pack

Detects Personally Identifiable Information (PII) patterns and computes sensitivity metrics.

### Metrics
- `pii_hits` per column
- `pii_columns` count
- `pii_records_ratio` at dataset scope

### Config
Patterns are configurable via `pack_conf.json` under `job.pii_patterns`.


#### Exemple de configuration (`pack_conf.json`)
