"""
Main file for pack
"""
import json
import warnings
import pandas as pd
import os
import utils
from ydata_profiling import ProfileReport
from datetime import datetime

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
aggregated_metrics = []
aggregated_alerts = []
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
        df,
        title=f"Profiling Report for {dataset_name}"
    )

    # Save the report to HTML
    html_file_name = f"{dataset_name}_report.html"
    profile.to_file(html_file_name)

    if source_config['type'] == 'file':
        source_file_dir = os.path.dirname(source_config['config']['path'])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(source_file_dir, f'{current_date}_profiling_report_{source_config["name"]}.html')

        profile.to_file(report_file_path)

        print(f"Profiling report saved to {report_file_path}")

    # Save the report to JSON
    json_file_name = f"{dataset_name}_report.json"
    profile.to_file(json_file_name)

    try:
        with open(html_file_name, "r", encoding="utf-8") as f:
            tables = pd.read_html(f.read())
    except ValueError as e:
        print(f"No tables found in the HTML report: {e}")
        tables = [pd.DataFrame()]  # Create an empty DataFrame if no tables are found

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
                "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}},
            }
        )


    # Load the JSON file
    print(f"Load {dataset_name}_report.json")
    with open(f"{dataset_name}_report.json", "r", encoding="utf-8") as file:
        report = json.load(file)

    general_data = utils.denormalize(report["table"])
    new_format_data = []
    for key, value in general_data.items():
        if source_config['type'] == 'database':
            entry = {
                "key": key,
                "value": utils.round_if_numeric(value),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name, "parent_scope": {"perimeter": "database", "value": source_config["name"]}},
            }
        else:
            entry = {
                "key": key,
                "value": utils.round_if_numeric(value),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name},
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
                "scope": {"perimeter": "column", "value": variable_name, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}},
            }
            new_format_data.append(entry)

    variables_data = new_format_data

    # Convert general_data to DataFrame
    general_data_df = pd.DataFrame(general_data)

    # Extract p_cells_missing value (as a decimal)
    p_cells_missing_value = general_data_df[
        general_data_df["key"] == "p_cells_missing"
    ]["value"].values[0]
    p_cells_missing = float(p_cells_missing_value)

    # Calculate the score
    score_value = 1 - p_cells_missing

    # Ensure the score is within the range [0, 1]
    score_value = max(min(score_value, 1), 0)

    # Creating the DataFrame for the score
    if source_config['type'] == 'database':
        completeness_scores.append(
            {
                "key": "score",
                "value": str(round(score_value, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_scope_name, "parent_scope": {"perimeter": "database", "value": source_config["name"]}},
            }
        )
    else:
        completeness_scores.append(
            {
                "key": "score",
                "value": str(round(score_value, 2)),
                "scope": {"perimeter": "dataset", "value": source_config["name"]},
            }
        )

    # Convert the completeness scores to DataFrame
    completeness_scores_df = pd.DataFrame(completeness_scores)

    ############################ Recommendations
    # Handle alerts
    if len(tables) > 2:  # Check if the expected table exists
        alerts_data = tables[
            2
        ]  # Adjust the index based on where the alerts are located
        alerts_data.columns = ["content", "type"]
        # Apply the extract_variable_name function to set the 'scope' column
        alerts_data["scope"] = alerts_data["content"].apply(
            lambda x: {"perimeter": "column", "value": utils.extract_variable_name(x), "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}}
        )

        # Apply the function to the 'content' column of the alerts DataFrame
        alerts_data["level"] = alerts_data["content"].apply(utils.determine_level)
    else:
        print("No alerts table found in the HTML report.")
        alerts_data = pd.DataFrame()  # Create an empty DataFrame if no alerts are found

    ############################ Render
    # Convert lists to DataFrames
    general_data_df = pd.DataFrame(general_data)
    variables_data_df = pd.DataFrame(variables_data)

    # Concatenate all the DataFrames
    metrics_data = pd.concat(
        [general_data_df, variables_data_df, completeness_scores_df],
        ignore_index=True,
    )

    ############################ Schemas
    schemas_data = []

    # Add entries for each variable
    for variable_name in report["variables"].keys():
        entry = {
            "key": "column",
            "value": variable_name,
            "scope": {"perimeter": "column", "value": variable_name, "parent_scope": {"perimeter": "dataset", "value": dataset_scope_name}},
        }
        schemas_data.append(entry)

    if source_config['type'] == 'database':
        schemas_data.append(
            {
                "key": "dataset",
                "value": dataset_scope_name,
                "scope": {"perimeter": "dataset", "value": dataset_scope_name, "parent_scope": {"perimeter": "database", "value": source_config["name"]}}
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

    # Append data for the current table to the aggregated lists
    aggregated_metrics.extend(metrics_data.to_dict("records"))
    aggregated_alerts.extend(alerts_data.to_dict("records"))
    aggregated_schemas.extend(schemas_data)

############################ Writing Results to Files

if source_config['type'] == 'database':
    aggregated_schemas.append(
        {
            "key": "database",
            "value": source_config["name"],
            "scope": {"perimeter": "database", "value": source_config["name"]},
        }
    )

    # Compute the aggregated database level completeness score from the datasets score
    aggregated_score = 0
    for metric in aggregated_metrics:
        if metric["key"] == "score":
            aggregated_score += float(metric["value"])
    aggregated_score /= len(df_dict)

    aggregated_metrics.append(
        {
            "key": "score",
            "value": str(round(aggregated_score, 2)),
            "scope": {"perimeter": "database", "value": source_config["name"]},
        }
    )

# Convert aggregated lists to DataFrames
metrics_df = pd.DataFrame(aggregated_metrics)
alerts_df = pd.DataFrame(aggregated_alerts)
schemas_df = pd.DataFrame(aggregated_schemas)

# Convert the DataFrames to JSON strings
metrics_json = metrics_df.to_json(orient="records")
alerts_json = alerts_df.to_json(orient="records")
schemas_json = schemas_df.to_json(orient="records")

# Write the JSON strings to files
with open("metrics.json", "w", encoding="utf-8") as f:
    json.dump(json.loads(metrics_json), f, indent=4)

with open("recommendations.json", "w", encoding="utf-8") as f:
    json.dump(json.loads(alerts_json), f, indent=4)

with open("schemas.json", "w", encoding="utf-8") as f:
    json.dump(json.loads(schemas_json), f, indent=4)

print("Processing completed.")
