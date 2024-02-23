from qalita_core.pack import Pack
from qalita_core.utils import (
    denormalize,
    round_if_numeric,
    extract_variable_name,
    determine_level,
)
import json
import pandas as pd
import os
from ydata_profiling import ProfileReport
from datetime import datetime
from io import StringIO

pack = Pack()
pack.load_data("source")

########################### Profiling and Aggregating Results
# Determine the appropriate name for 'dataset' in 'scope'
dataset_scope_name = pack.source_config["name"]

print(f"Generating profile for {dataset_scope_name}")

# Run the profiling report
profile = ProfileReport(
    pack.df_source,
    title=f"Profiling Report for {dataset_scope_name}",
    correlations={"auto": {"calculate": False}},
)

# Save the report to HTML
html_file_name = f"{dataset_scope_name}_report.html"
profile.to_file(html_file_name)

if pack.source_config["type"] == "file":
    source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
    current_date = datetime.now().strftime("%Y%m%d")
    report_file_path = os.path.join(
        source_file_dir,
        f'{current_date}_profiling_report_{pack.source_config["name"]}.html',
    )

    profile.to_file(report_file_path)

    print(f"Profiling report saved to {report_file_path}")

# Save the report to JSON
json_file_name = f"{dataset_scope_name}_report.json"
profile.to_file(json_file_name)

try:
    with open(html_file_name, "r", encoding="utf-8") as f:
        html_content = f.read()
        tables = pd.read_html(StringIO(html_content)) 
except ValueError as e:
    print(f"No tables found in the HTML report: {e}")
    tables = [pd.DataFrame()]  # Create an empty DataFrame if no tables are found

############################ Metrics

# Calculate the completeness score for each column
for col in pack.df_source.columns:
    non_null_count = max(pack.df_source[col].notnull().sum(), 0)  # Ensure non-negative
    total_count = max(len(pack.df_source), 1)  # Ensure non-zero and non-negative
    completeness_score = round(non_null_count / total_count, 2)
    pack.metrics.data.append(
        {
            "key": "completeness_score",
            "value": str(completeness_score),
            "scope": {
                "perimeter": "column",
                "value": col,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": dataset_scope_name,
                },
            },
        }
    )

# Load the JSON file
print(f"Load {dataset_scope_name}_report.json")
with open(f"{dataset_scope_name}_report.json", "r", encoding="utf-8") as file:
    report = json.load(file)

general_data = denormalize(report["table"])
for key, value in general_data.items():
    if pack.source_config["type"] == "database":
        entry = {
            "key": key,
            "value": round_if_numeric(value),
            "scope": {
                "perimeter": "dataset",
                "value": dataset_scope_name,
                "parent_scope": {
                    "perimeter": "database",
                    "value": pack.source_config["name"],
                },
            },
        }
    else:
        entry = {
            "key": key,
            "value": round_if_numeric(value),
            "scope": {"perimeter": "dataset", "value": dataset_scope_name},
        }
    pack.metrics.data.append(entry)

variables_data = report["variables"]
for variable_name, attributes in variables_data.items():
    for attr_name, attr_value in attributes.items():
        entry = {
            "key": attr_name,
            "value": round_if_numeric(attr_value),
            "scope": {
                "perimeter": "column",
                "value": variable_name,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": dataset_scope_name,
                },
            },
        }
        pack.metrics.data.append(entry)

# Extract p_cells_missing value (as a decimal)
df_missing = pd.DataFrame(pack.metrics.data)
p_cells_missing_value = df_missing[df_missing["key"] == "p_cells_missing"][
    "value"
].values[0]
p_cells_missing = float(p_cells_missing_value)

# Calculate the score
score_value = 1 - p_cells_missing

# Ensure the score is within the range [0, 1]
score_value = max(min(score_value, 1), 0)

# Creating the DataFrame for the score
if pack.source_config["type"] == "database":
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(score_value, 2)),
            "scope": {
                "perimeter": "dataset",
                "value": dataset_scope_name,
                "parent_scope": {
                    "perimeter": "database",
                    "value": pack.source_config["name"],
                },
            },
        }
    )
else:
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(score_value, 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )

# Convert the completeness scores to DataFrame
completeness_scores_df = pd.DataFrame(pack.metrics.data)

############################ Recommendations
# Handle alerts
if len(tables) > 2:  # Check if the expected table exists
    alerts_data = tables[2]  # Adjust the index based on where the alerts are located
    alerts_data.columns = ["content", "type"]
    # Apply the extract_variable_name function to set the 'scope' column
    alerts_data["scope"] = alerts_data["content"].apply(
        lambda x: {
            "perimeter": "column",
            "value": extract_variable_name(x),
            "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name},
        }
    )

    # Apply the function to the 'content' column of the alerts DataFrame
    alerts_data["level"] = alerts_data["content"].apply(determine_level)
else:
    print("No alerts table found in the HTML report.")
    alerts_data = pd.DataFrame()  # Create an empty DataFrame if no alerts are found

alerts_list_of_dicts = alerts_data.to_dict(orient="records")
pack.recommendations.data = alerts_list_of_dicts

############################ Schemas
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
    pack.schemas.data.append(entry)

if pack.source_config["type"] == "database":
    pack.schemas.data.append(
        {
            "key": "dataset",
            "value": dataset_scope_name,
            "scope": {
                "perimeter": "dataset",
                "value": dataset_scope_name,
                "parent_scope": {
                    "perimeter": "database",
                    "value": pack.source_config["name"],
                },
            },
        }
    )
else:
    pack.schemas.data.append(
        {
            "key": "dataset",
            "value": dataset_scope_name,
            "scope": {"perimeter": "dataset", "value": dataset_scope_name},
        }
    )

############################ Writing Results to Files

# if pack.source_config["type"] == "database":
#     pack.schemas.data.append(
#         {
#             "key": "database",
#             "value": pack.source_config["name"],
#             "scope": {"perimeter": "database", "value": pack.source_config["name"]},
#         }
#     )

#     # Compute the aggregated database level completeness score from the datasets score
#     aggregated_score = 0
#     for metric in pack.metrics.data:
#         if metric["key"] == "score":
#             aggregated_score += float(metric["value"])
#     aggregated_score /= len(df_dict)

#     pack.metrics.data.append(
#         {
#             "key": "score",
#             "value": str(round(aggregated_score, 2)),
#             "scope": {"perimeter": "database", "value": pack.source_config["name"]},
#         }
#     )

pack.metrics.save()
pack.recommendations.save()
pack.schemas.save()
