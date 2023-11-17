"""
Main file for pack
"""
import os
import glob
import json
import sys
import warnings
import pandas as pd
from ydata_profiling import ProfileReport

warnings.filterwarnings("ignore", category=DeprecationWarning)


def denormalize(data):
    """
    Denormalize a dictionary with nested dictionaries
    """
    denormalized = {}
    for index, content in data.items():
        if isinstance(content, dict):
            for inner_key, inner_value in content.items():
                new_key = f"{index}_{inner_key.lower()}"
                denormalized[new_key] = inner_value
        else:
            denormalized[index] = content
    return denormalized


# Load the configuration file
print("Load source_conf.json")
with open("source_conf.json", "r", encoding="utf-8") as file:
    config = json.load(file)

# Check if data source is supported
if config["type"] != "file":
    print("Source Type not supported")
    sys.exit(1)

# Get the path from the config file
path = config["config"]["path"]

# Check if there are CSV files in the path
print("Check csv files")
csv_files = glob.glob(os.path.join(path, "*.csv"))

if csv_files:
    print("CSV files found:")
    first_csv_file = csv_files[0]
    print(f"Loading first CSV file: {first_csv_file}")
    df = pd.read_csv(
        first_csv_file, low_memory=False, memory_map=True, on_bad_lines="skip"
    )
else:
    raise FileNotFoundError("No CSV files found in the provided path.")

profile = ProfileReport(df, minimal=True, title="Profiling Report")
profile.to_file("report.html")
tables = pd.read_html("report.html")
profile.to_file("report.json")

############################ Metrics

# Load the JSON file
print("Load report.json")
with open("report.json", "r", encoding="utf-8") as file:
    report = json.load(file)

general_data = denormalize(report["table"])
new_format_data = []
for key, value in general_data.items():
    entry = {
        "key": key,
        "value": str(value),
        "scope": {"perimeter": "dataset", "value": config["name"]},
    }
    new_format_data.append(entry)
general_data = new_format_data

variables_data = report["variables"]
new_format_data = []
for variable_name, attributes in variables_data.items():
    for attr_name, attr_value in attributes.items():
        entry = {
            "key": attr_name,
            "value": str(attr_value),
            "scope": {"perimeter": "column", "value": variable_name},
        }
        new_format_data.append(entry)

variables_data = new_format_data

# Convert general_data to DataFrame
general_data_df = pd.DataFrame(general_data)

# Get missing_cells and number_of_observations
missing_cells = int(
    general_data_df[general_data_df["key"] == "n_cells_missing"]["value"].values[0]
)
number_of_observations = int(
    general_data_df[general_data_df["key"] == "n"]["value"].values[0]
)

score = pd.DataFrame(
    {
        "key": "score",
        "value": str(
            (int(number_of_observations) - int(missing_cells))
            / int(number_of_observations)
        ),
        "scope": {"perimeter": "dataset", "value": config["name"]},
    },
    index=[0],
)

############################ Recommendations

alerts = tables[2]
alerts.columns = ["content", "type"]
alerts["scope"] = {"perimeter": "global"}
# Set the scope perimeter as 'column'
alerts["scope"] = alerts["content"].str.split().str[0]

# Convert the scope to JSON
alerts["scope"] = alerts["scope"].apply(lambda x: {"perimeter": "column", "value": x})

############################ Schemas

# Initialize the list with the dataset name entry
schemas_data = [
    {
        "key": "dataset",
        "value": config["name"],
        "scope": {"perimeter": "dataset", "value": config["name"]},
    }
]

# Add entries for each variable
for variable_name in report["variables"].keys():
    entry = {
        "key": "column",
        "value": variable_name,
        "scope": {"perimeter": "column", "value": variable_name},
    }
    schemas_data.append(entry)

# Convert lists to DataFrames
general_data_df = pd.DataFrame(general_data)
variables_data_df = pd.DataFrame(variables_data)

# Concatenate all the DataFrames
metrics = pd.concat([general_data_df, variables_data_df, score], ignore_index=True)

# Convert the DataFrames to JSON strings
metrics_json = metrics.to_json(orient="records")
alerts_json = alerts.to_json(orient="records")

# Load the JSON strings to Python objects
metrics_data = json.loads(metrics_json)
alerts_data = json.loads(alerts_json)

# Write the Python objects to files in pretty format
with open("metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics_data, f, indent=4)

with open("recommendations.json", "w", encoding="utf-8") as f:
    json.dump(alerts_data, f, indent=4)

with open("schemas.json", "w", encoding="utf-8") as f:
    json.dump(schemas_data, f, indent=4)
