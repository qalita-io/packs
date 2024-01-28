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

############################ Metrics

# # Calculate the duplicates score for each column
# duplicates_scores = []
# # Updated Duplicate Score Calculation
# for col in df.columns:
#     duplicates = df.duplicated(subset=col).sum()
#     total_count = len(df)
#     duplicates_score = duplicates / total_count
#     duplicates_scores.append({
#         "key": "duplicates_score",
#         "value": duplicates_score,
#         "scope": {"perimeter": "column", "value": col}
#     })

# # Convert the completeness scores to DataFrame
# duplicates_scores_df = pd.DataFrame(duplicates_scores)

# Calculate the total number of duplicate rows in the dataset
total_duplicates = df.duplicated().sum()
total_rows = len(df)

# Calculate the original duplication score
original_duplication_score = round(total_duplicates / total_rows if total_rows > 0 else 0, 2)

# Invert the score
inverted_duplication_score = 1 - original_duplication_score

# Add the inverted duplication score to the metrics
aggregated_score_entry = {
    "key": "score",
    "value": inverted_duplication_score,
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
}

aggregated_score_df = pd.DataFrame([aggregated_score_entry])

############################ Recommendations

recommendations = []

if inverted_duplication_score < 0.9:
    recommendation = {
        "content": f"dataset '{source_config['name']}' has a duplication rate of {original_duplication_score*100}%. Consider reviewing for data cleaning.",
        "type": "Duplicates",
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
        "level": utils.determine_recommendation_level(original_duplication_score)
    }
    recommendations.append(recommendation)

# # Generate Recommendations Based on Duplicate Scores
# for score in duplicates_scores:
#     column_name = score["scope"]["value"]
#     dup_score = score["value"]
#     dup_rate_percentage = round(dup_score * 100, 2)  # Convert to percentage

#     # Only add recommendations for significant duplication rates
#     if dup_score > 0.1:  # Adjust this threshold as needed
#         recommendation = {
#             "content": f"Column '{column_name}' has a duplication rate of {dup_rate_percentage}%. Consider reviewing for data cleaning.",
#             "type": "Duplicates",
#             "scope": {"perimeter": "column", "value": column_name},
#             "level": utils.determine_recommendation_level(dup_score)
#         }
#         recommendations.append(recommendation)

# Convert the recommendations list to a DataFrame
recommendations_df = pd.DataFrame(recommendations)

############################ Schemas

schemas_data = []

# Concatenate all the DataFrames
# metrics = pd.concat([duplicates_scores_df, aggregated_score_df], ignore_index=True)
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

with open("schemas.json", "w", encoding="utf-8") as f:
    json.dump(schemas_data, f, indent=4)
