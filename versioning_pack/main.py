import os
import sys
import requests
from qalita_core.pack import Pack

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
pack = Pack()
if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

# Extract API URL and token
api_url = pack.agent_config["context"]["local"]["url"]
api_token = pack.agent_config["context"]["local"]["token"]

############################ Metrics

# Fetch remote schema data from the API
source_id = pack.source_config["id"]

# Get the api token from the env variable or use the one from the agent file
token = os.getenv("QALITA_AGENT_TOKEN", api_token)
endpoint = os.getenv("QALITA_AGENT_ENDPOINT", api_url)

# get source infos
response_source = requests.get(
    f"{endpoint}/sources/{source_id}",
    headers={"Authorization": f"Bearer {token}"},
)
if response_source.status_code == 200:
    source = response_source.json()
else:
    print(f"Failed to fetch source, status code: {response_source.status_code}")
    sys.exit(1)

# get source_versions
source_versions = source["versions"]

# get latest version
latest_version = source_versions[-1]

# get latest version id
latest_version_id = latest_version["id"]

# request the schema API
response_schema = requests.get(
    f"{endpoint}/schemas/{source_id}?source_version_id={latest_version_id}",
    headers={"Authorization": f"Bearer {token}"},
)
if response_schema.status_code == 200:
    schema = response_schema.json()
else:
    print(f"Failed to fetch schema, status code: {response_schema.status_code}")
    sys.exit(1)

raw_df_source = pack.df_source
if isinstance(raw_df_source, list):
    # For multiple datasets, just compare the first one for versioning purpose
    local_columns = set(raw_df_source[0].columns)
else:
    local_columns = set(raw_df_source.columns)

# Remote schema
remote_columns = set(item["value"] for item in schema if item["key"] == "column")

# Compare
if local_columns == remote_columns:
    print("Schemas match.")
else:
    print("Schemas do not match.")

# If same data model, same version

# If different data model, new version

# Increase the version of the source with a minor version
# 1.0.0 -> 1.1.0

# Push the source with the new version to the API


# Utility function to bump version
def bump_version(version):
    major, minor, patch = map(int, version.split("."))
    return f"{major}.{minor + 1}.{patch}"


# Set current version from latest_version fetched from remote
current_version = latest_version["sem_ver_id"]

# Compute the new version based on schema match
if local_columns == remote_columns:
    computed_version = current_version
    print(f"Schemas match. Version remains {current_version}")
else:
    computed_version = bump_version(current_version)
    print(f"Schemas do not match. Version bumped to {computed_version}")

# Create metrics data
pack.metrics.data.extend(
    [
        {
            "key": "current_version",
            "value": current_version,
            "scope": {
                "perimeter": "dataset",
                "value": "Medical Cost Personal Datasets",
            },
        },
        {
            "key": "computed_version",
            "value": computed_version,
            "scope": {
                "perimeter": "dataset",
                "value": "Medical Cost Personal Datasets",
            },
        },
    ]
)

pack.metrics.save()
