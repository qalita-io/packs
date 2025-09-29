## Versioning Pack

### Overview
Compares local dataset schema to the remote schema and decides whether to bump the version. Emits current and computed version metrics.

### How it works
- Loads the source dataset and its columns (for multi-table databases, uses the first dataset).
- Calls the API to fetch the latest source version and its schema.
- Compares local vs remote columns; if equal, keep version; otherwise, bump minor version.

### Configuration
- `QALITA_AGENT_TOKEN` and `QALITA_AGENT_ENDPOINT` via env for API access.
- `source_conf.json` must contain the `id` used by the API.

### Outputs
- `metrics.json`: includes `current_version` (from remote) and `computed_version` (same or bumped minor).

### Contribute
This pack is part of QALITA Open Source Assets (QOSA). Contributions are welcome: https://github.com/qalita-io/packs.
