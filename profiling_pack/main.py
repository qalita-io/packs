"""
Main file for pack
"""
import json
import warnings
import pandas as pd
from ydata_profiling import ProfileReport
import utils

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

df = load_data(source_config, pack_config)

########################### Profiling

# Run the profiling report
profile = ProfileReport(df, minimal=True, title="Profiling Report")
profile.to_file("report.html")
tables = pd.read_html("report.html")
profile.to_file("report.json")

############################ Metrics

# Calculate the completeness score for each column
completeness_scores = []
for col in df.columns:
    non_null_count = max(df[col].notnull().sum(), 0)  # Ensure non-negative
    total_count = max(len(df), 1)  # Ensure non-zero and non-negative
    completeness_score = round(non_null_count / total_count, 2)
    completeness_scores.append(
        {
            "key": "completeness_score",
            "value": str(completeness_score),
            "scope": {"perimeter": "column", "value": col},
        }
    )

# Convert the completeness scores to DataFrame
completeness_scores_df = pd.DataFrame(completeness_scores)

# Load the JSON file
print("Load report.json")
with open("report.json", "r", encoding="utf-8") as file:
    report = json.load(file)

general_data = utils.denormalize(report["table"])
new_format_data = []
for key, value in general_data.items():
    entry = {
        "key": key,
        "value": utils.round_if_numeric(value),
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
    }
    new_format_data.append(entry)
general_data = new_format_data

variables_data = report["variables"]
new_format_data = []
for variable_name, attributes in variables_data.items():
    for attr_name, attr_value in attributes.items():
        entry = {
            "key": attr_name,
            "value": utils.round_if_numeric(attr_value),
            "scope": {"perimeter": "column", "value": variable_name},
        }
        new_format_data.append(entry)

variables_data = new_format_data

# Convert general_data to DataFrame
general_data_df = pd.DataFrame(general_data)

# Extract p_cells_missing value (as a decimal)
p_cells_missing_value = general_data_df[general_data_df["key"] == "p_cells_missing"][
    "value"
].values[0]
p_cells_missing = float(p_cells_missing_value)

# Calculate the score
score_value = 1 - p_cells_missing

# Ensure the score is within the range [0, 1]
score_value = max(min(score_value, 1), 0)

# Creating the DataFrame for the score
score = pd.DataFrame(
    {
        "key": "score",
        "value": str(round(score_value, 2)),
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
    },
    index=[0],
)

############################ Recommendations

alerts = tables[2]
alerts.columns = ["content", "type"]

# Apply the extract_variable_name function to set the 'scope' column
alerts["scope"] = alerts["content"].apply(
    lambda x: {"perimeter": "column", "value": utils.extract_variable_name(x)}
)

# Apply the function to the 'content' column of the alerts DataFrame
alerts["level"] = alerts["content"].apply(utils.determine_level)

############################ Schemas

# Initialize the list with the dataset name entry
schemas_data = [
    {
        "key": "dataset",
        "value": source_config["name"],
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
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
metrics = pd.concat(
    [general_data_df, variables_data_df, score, completeness_scores_df],
    ignore_index=True,
)

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
