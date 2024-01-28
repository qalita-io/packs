import os
import json
import sys
import warnings
import requests
import base64
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load the .qalita/.agent file
agent_file_path = os.path.expanduser('~/.qalita/.agent')
with open(agent_file_path, 'r') as agent_file:
    encoded_content = agent_file.read()

# Decode from base64
decoded_content = base64.b64decode(encoded_content).decode('utf-8')

# Load as JSON
agent_config = json.loads(decoded_content)

# Extract API URL and token
api_url = agent_config['context']['local']['url']
api_token = agent_config['context']['local']['token']

########################### Loading Data

# Load the configuration file
print("Load source_conf.json")
with open("source_conf.json", "r", encoding="utf-8") as file:
    source_config = json.load(file)

# Load the pack configuration file
print("Load pack_conf.json")
with open("pack_conf.json", "r", encoding="utf-8") as file:
    pack_config = json.load(file)

# Load data using the opener.py logic
from opener import load_data
df = load_data(source_config, pack_config)

############################ Metrics

# Fetch remote schema data from the API
source_id = source_config["id"]

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
source_versions = source['versions']

# get latest version
latest_version = source_versions[-1]

# get latest version id
latest_version_id = latest_version['id']

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

# Local CSV schema
local_columns = set(df.columns)

# Remote schema
remote_columns = set(item['value'] for item in schema if item['key'] == 'column')

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
    major, minor, patch = map(int, version.split('.'))
    return f"{major}.{minor + 1}.{patch}"

# Set current version from latest_version fetched from remote
current_version = latest_version['sem_ver_id']

# Compute the new version based on schema match
if local_columns == remote_columns:
    computed_version = current_version
    print(f"Schemas match. Version remains {current_version}")
else:
    computed_version = bump_version(current_version)
    print(f"Schemas do not match. Version bumped to {computed_version}")

# Create metrics data
metrics_data = [
    {
        "key": "current_version",
        "value": current_version,
        "scope": {
            "perimeter": "dataset",
            "value": "Medical Cost Personal Datasets"
        }
    },
    {
        "key": "computed_version",
        "value": computed_version,
        "scope": {
            "perimeter": "dataset",
            "value": "Medical Cost Personal Datasets"
        }
    }
]

# Save to metrics.json file
with open("metrics.json", "w") as f:
    json.dump(metrics_data, f, indent=4)

print("Metrics saved to metrics.json")
