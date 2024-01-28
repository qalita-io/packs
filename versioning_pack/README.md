# Versioning Pack

## Overview

This pack is designed for data quality management, focusing on schema validation and version control of datasets. It automates the process of loading data, verifying schema consistency between local and remote sources, and managing dataset versioning.

## Functionality

### Loading Data
- **Configuration Loading**: The pack starts by loading configuration details from `source_conf.json` and `pack_conf.json`.
- **Data Loading**: Utilizes the `opener.py` to load data based on the provided configuration.

### Metrics and Schema Validation
- **Fetching Remote Schema**: Connects to a remote API to fetch schema data for the specified source.
- **Schema Comparison**: Compares the local dataset schema with the remote schema to ensure consistency.
- **Version Management**:
  - If the schemas match, the current version is retained.
  - If the schemas differ, a new version is created by incrementing the minor version number.
- **Metrics Generation**: Generates and stores key metrics like `current_version` and `computed_version` in `metrics.json`.

## Components

1. **Configuration Files**:
   - `source_conf.json`: Contains source-specific configuration.
   - `pack_conf.json`: Contains pack-specific configuration.

2. **Python Libraries**:
   - Standard libraries: `os`, `glob`, `json`, `sys`.
   - Data handling: `pandas`.
   - HTTP requests: `requests`.
   - Warning control: `warnings`.

3. **Utility Functions**:
   - `load_data`: Loads data based on source and pack configuration.
   - `bump_version`: Utility function to increment the dataset version.

4. **Environmental Variables**:
   - `QALITA_AGENT_TOKEN`: Token for authentication with the remote API.
   - `QALITA_AGENT_ENDPOINT`: Endpoint URL for the remote API.

## Workflow

1. **Initialization**:
   - Load configuration files.
   - Load data using the defined logic in `opener.py`.

2. **Schema Fetching and Validation**:
   - Fetch the remote schema using the API.
   - Compare the local and remote schemas.
   - Manage dataset versions based on schema comparison results.

3. **Metrics Handling**:
   - Generate and save metrics data in `metrics.json`.

4. **Error Handling**:
   - Proper handling and logging for fetching errors from the API.
   - Systematic exit if critical errors occur.

## Usage

1. Ensure environmental variables `QALITA_AGENT_TOKEN` and `QALITA_AGENT_ENDPOINT` are set.
2. Run the pack script.
3. Check `metrics.json` for the generated metrics.

## Error Handling

- The pack includes error handling for failed API requests, ensuring that appropriate messages are logged and the process is exited cleanly in case of critical failures.

---

Please ensure that you have the correct permissions and valid API tokens before running this pack. Also, verify the schema and version details in the respective configuration files to avoid inconsistencies.