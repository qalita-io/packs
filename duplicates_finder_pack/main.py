"""
Main file for pack
"""
import json
import warnings
import pandas as pd
import utils
import os
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

df = load_data(source_config, pack_config)

# Define uniqueness_columns if not specified in pack_config
if "job" in pack_config and "compute_uniqueness_columns" in pack_config["job"]:
    uniqueness_columns = pack_config["job"]["compute_uniqueness_columns"]
else:
    uniqueness_columns = df.columns

############################ Metrics

# Step 1: Filter the DataFrame based on the specified columns
print("Columns used for checking duplicates:", uniqueness_columns)
df_subset = df[uniqueness_columns]
total_rows = len(df)

# Step 2: Calculate the number of duplicate rows based on this subset
total_duplicates = df_subset.duplicated().sum()

# Calculate the scoped duplication score
duplication_score = round(total_duplicates / total_rows if total_rows > 0 else 0, 2)

# Invert the score for the scoped scenario
score = 1 - duplication_score

# Add the inverted duplication score to the metrics
aggregated_score_entry = {
    "key": "score",
    "value": score,
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
}

aggregated_score_df = pd.DataFrame([aggregated_score_entry])

# Create metric entries as DataFrames
total_duplicates_df = pd.DataFrame([{
    "key": "duplicates",
    "value": total_duplicates,
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
}])
# Use pd.concat to add the total duplicates entry to the metrics
aggregated_score_df = pd.concat([aggregated_score_df, total_duplicates_df], ignore_index=True)

# Add the total duplicates entry to the metrics
aggregated_score_df = pd.concat([aggregated_score_df, total_duplicates_df], ignore_index=True)

# Check if scoped score is calculated and add its metrics
if "job" in pack_config and "compute_uniqueness_columns" in pack_config["job"]:
    scoped_duplicates_df = pd.DataFrame([{
        "key": "duplicates",
        "value": total_duplicates,
        "scope": {"perimeter": "dataset", "value": ", ".join(uniqueness_columns)},
    }])
    aggregated_score_df = pd.concat([aggregated_score_df, scoped_duplicates_df], ignore_index=True)

############################ Recommendations

recommendations = []

if score < 0.9:

    recommendation = {
        "content": f"dataset '{source_config['name']}' has a duplication rate of {duplication_score*100}%. on the scope {uniqueness_columns} .",
        "type": "Duplicates",
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
        "level": utils.determine_recommendation_level(duplication_score)
    }
    recommendations.append(recommendation)

# Convert the recommendations list to a DataFrame
recommendations_df = pd.DataFrame(recommendations)

# Concatenate all the DataFrames
metrics = pd.concat([aggregated_score_df], ignore_index=True)

# Convert the DataFrames to JSON strings
metrics_json = metrics.to_json(orient="records")
recommendations_json = recommendations_df.to_json(orient="records")

# Load the JSON strings to Python objects
metrics_data = json.loads(metrics_json)
recommendations_data = json.loads(recommendations_json)

# Write the Python objects to files in pretty format
with open("metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics_data, f, indent=4)

with open("recommendations.json", "w", encoding="utf-8") as f:
    json.dump(recommendations_data, f, indent=4)

######################## Export:
# Step 1: Retrieve 'id_columns' from pack_config
id_columns = pack_config.get('job', {}).get('id_columns', [])

# Check if uniqueness_columns is empty and handle accordingly
if not uniqueness_columns:
    print("No columns specified for checking duplicates. Using all columns.")
    uniqueness_columns = df.columns.tolist()  # Use all columns if none are specified

# Step 2: Identify duplicated rows
duplicated_rows = df[df.duplicated(subset=uniqueness_columns, keep=False)]

# Check if there are any duplicates
if duplicated_rows.empty:
    print("No duplicates found. No report will be generated.")
else:
    # If there are duplicates, proceed with sorting and exporting
    duplicated_rows = duplicated_rows.sort_values(by=uniqueness_columns)

    # Step 3: Set index or create 'index' column for the Excel export
    if id_columns:
        # Ensure all id_columns are in the DataFrame columns
        valid_id_columns = [col for col in id_columns if col in duplicated_rows.columns]
        if not valid_id_columns:
            print("None of the specified 'id_columns' are in the DataFrame. Using default index.")
            duplicated_rows = duplicated_rows.reset_index(drop=True)
        else:
            duplicated_rows = duplicated_rows.set_index(valid_id_columns)
    else:
        # If 'id_columns' is not provided or is empty, create an 'index' column with the original DataFrame's index
        duplicated_rows = duplicated_rows.reset_index()

    # Continue with the export process
    if source_config['type'] == 'file':
        source_file_dir = os.path.dirname(source_config['config']['path'])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(source_file_dir, f'{current_date}_duplicates_finder_report_{source_config["name"]}.xlsx')

        # Export duplicated rows to an Excel file
        duplicated_rows.to_excel(report_file_path, index=False)  # Set index=False as 'original_index' is now a column
        print(f"Duplicated rows have been exported to {report_file_path}")
