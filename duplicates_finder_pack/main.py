"""
Main file for pack
"""
import json
import warnings
import pandas as pd
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

if "job" in pack_config and "compute_uniqueness_columns" in pack_config["job"]:
    # Compute the uniqueness columns
    uniqueness_columns = pack_config["job"]["compute_uniqueness_columns"]
else:
    # If the list of columns is not specified, use all date columns
    uniqueness_columns = df.columns

recommendations = []

############################ Metrics

# Calculate the total number of duplicate rows in the dataset
total_duplicates = df.duplicated().sum()
total_rows = len(df)

# Calculate the original duplication score
original_duplication_score = round(total_duplicates / total_rows if total_rows > 0 else 0, 2)

# Invert the score
inverted_duplication_score = 1 - original_duplication_score

# Step 1: Filter the DataFrame based on the specified columns
df_subset = df[uniqueness_columns]

# Step 2: Calculate the number of duplicate rows based on this subset
total_duplicates_subset = df_subset.duplicated().sum()

# Calculate the scoped duplication score
scoped_duplication_score = round(total_duplicates_subset / total_rows if total_rows > 0 else 0, 2)

# Invert the score for the scoped scenario
scoped_score = 1 - scoped_duplication_score

# Use the scoped_score if the compute_uniqueness_columns is specified in the pack_config
if "job" in pack_config and "compute_uniqueness_columns" in pack_config["job"]:
    score = scoped_score
    # Add more details to the recommendation if the scoped_score is used
    if score < 0.9:
        uniqueness_columns_str = ", ".join(uniqueness_columns)
        recommendation = {
            "content": f"dataset '{source_config['name']}' has a high duplication rate of {scoped_duplication_score*100}% based on the subset of columns: {uniqueness_columns_str}. Consider reviewing these columns for data cleaning.",
            "type": "Duplicates",
            "scope": {"perimeter": "dataset", "value": source_config["name"]},
            "level": utils.determine_recommendation_level(scoped_duplication_score)
        }
        recommendations.append(recommendation)
else:
    score = inverted_duplication_score

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
        "value": total_duplicates_subset,
        "scope": {"perimeter": "dataset", "value": ", ".join(uniqueness_columns)},
    }])
    aggregated_score_df = pd.concat([aggregated_score_df, scoped_duplicates_df], ignore_index=True)

############################ Recommendations

if score < 0.9:
    recommendation = {
        "content": f"dataset '{source_config['name']}' has a duplication rate of {original_duplication_score*100}%. Consider reviewing for data cleaning.",
        "type": "Duplicates",
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
        "level": utils.determine_recommendation_level(original_duplication_score)
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