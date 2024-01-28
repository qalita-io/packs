"""
Main file for pack
"""
import json
import warnings
import pandas as pd
from ydata_profiling import ProfileReport

warnings.filterwarnings("ignore", category=DeprecationWarning)

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

df_dict = load_data(source_config, pack_config)

########################### Profiling and Aggregating Results

# Initialize lists to store aggregated results
aggregated_schemas = []

# Ensure that data is loaded
if not df_dict:
    raise ValueError("No data loaded from the source.")

# Iterate over each dataset and create profile reports
for dataset_name, df in df_dict.items():
    # Determine the appropriate name for 'dataset' in 'scope'
    dataset_scope_name = (
        dataset_name if source_config["type"] == "database" else source_config["name"]
    )

    print(f"Generating profile for {dataset_name}")

    # Run the profiling report
    profile = ProfileReport(
        df, minimal=True, title=f"Profiling Report for {dataset_name}"
    )

    # Save the report to HTML
    html_file_name = f"{dataset_name}_report.html"
    profile.to_file(html_file_name)

    # Save the report to JSON
    json_file_name = f"{dataset_name}_report.json"
    profile.to_file(json_file_name)

    try:
        with open(html_file_name, "r", encoding="utf-8") as f:
            tables = pd.read_html(f.read())
    except ValueError as e:
        print(f"No tables found in the HTML report: {e}")
        tables = [pd.DataFrame()]  # Create an empty DataFrame if no tables are found

    # Load the JSON file
    print(f"Load {dataset_name}_report.json")
    with open(f"{dataset_name}_report.json", "r", encoding="utf-8") as file:
        report = json.load(file)

    ############################ Schemas
    schemas_data = []

    # Add entries for each variable
    for variable_name in report["variables"].keys():
        entry = {
            "key": "column",
            "value": variable_name,
            "scope": {
                "perimeter": "column",
                "value": variable_name,
                "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name},
            },
        }
        schemas_data.append(entry)

    if source_config["type"] == "database":
        schemas_data.append(
            {
                "key": "dataset",
                "value": dataset_scope_name,
                "scope": {
                    "perimeter": "dataset",
                    "value": dataset_scope_name,
                    "parent_scope": {
                        "perimeter": "database",
                        "value": source_config["name"],
                    },
                },
            }
        )
    else:
        schemas_data.append(
            {
                "key": "dataset",
                "value": dataset_scope_name,
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
            }
        )

    aggregated_schemas.extend(schemas_data)

############################ Writing Results to Files

if source_config["type"] == "database":
    aggregated_schemas.append(
        {
            "key": "database",
            "value": source_config["name"],
            "scope": {"perimeter": "database", "value": source_config["name"]},
        }
    )

# Convert aggregated lists to DataFrames
schemas_df = pd.DataFrame(aggregated_schemas)

# Convert the DataFrames to JSON strings
schemas_json = schemas_df.to_json(orient="records")

# Write the JSON strings to files
with open("schemas.json", "w", encoding="utf-8") as f:
    json.dump(json.loads(schemas_json), f, indent=4)

print("Processing completed.")
