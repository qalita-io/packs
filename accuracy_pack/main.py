import json
import utils
import os
from datetime import datetime

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

############################ Compute Precision Score for Each Float Column
def compute_metrics(df):
    float_columns = df.select_dtypes(include=["float", "float64"]).columns

    # If there are no float columns, return None
    if not float_columns.any():
        print("No float columns found. metrics.json will not be created.")
        return []

    # Compute precision score for each float column
    precision_data = []
    total_proportion_score = 0  # Initialize total proportion score

    for column in float_columns:
        decimals_count = (
            df[column]
            .dropna()
            .apply(lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0)
        )
        max_decimals = decimals_count.max()
        most_common_decimals_series = decimals_count.mode()

        # Handle the scenario when the mode() returns an empty series
        if most_common_decimals_series.empty:
            print(f"No common decimal count found for column {column}.")
            most_common_decimals = 0
            proportion_score = 0
        else:
            most_common_decimals = most_common_decimals_series[
                0
            ]  # Get the most common decimals count
            proportion_score = decimals_count[
                decimals_count == most_common_decimals
            ].count() / len(decimals_count)

        total_proportion_score += proportion_score  # Add proportion score to the total

        precision_data.append(
            {
                "key": "decimal_precision",
                "value": str(max_decimals),  # Maximum number of decimals
                "scope": {"perimeter": "column", "value": column},
            }
        )

        precision_data.append(
            {
                "key": "proportion_score",
                "value": str(
                    round(proportion_score, 2)
                ),  # Proportion of values with the most common decimals count
                "scope": {"perimeter": "column", "value": column},
            }
        )

    # Calculate the mean of proportion scores
    mean_proportion_score = (
        total_proportion_score / len(float_columns) if float_columns.any() else 0
    )

    # Add the mean proportion score to the precision data
    precision_data.append(
        {
            "key": "score",
            "value": str(round(mean_proportion_score, 2)),  # Mean proportion score
            "scope": {"perimeter": "dataset", "value": source_config["name"]},
        }
    )

    return precision_data


# Compute metrics
precision_metrics = compute_metrics(df)

################### Recommendations
recommendations = []
for column in df.columns:
    for item in precision_metrics:
        if item["scope"]["value"] == column and item["key"] == "proportion_score":
            proportion_score = float(item["value"])
            if proportion_score < 0.9:
                recommendation = {
                    "content": f"Column '{column}' has {(1-proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                    "type": "Duplicates",
                    "scope": {"perimeter": "column", "value": column},
                    "level": utils.determine_recommendation_level(1 - proportion_score),
                }
                recommendations.append(recommendation)

# Recommendation for the dataset
if precision_metrics:
    mean_proportion_score = float(precision_metrics[-1]["value"])
    if mean_proportion_score < 0.9:
        recommendation = {
            "content": f"The dataset has {(1-mean_proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
            "type": "Duplicates",
            "scope": {"perimeter": "dataset", "value": source_config["name"]},
            "level": utils.determine_recommendation_level(1 - mean_proportion_score),
        }
        recommendations.append(recommendation)

############################ Writing Metrics and Recommendations to Files

if precision_metrics is not None:
    with open("metrics.json", "w") as file:
        json.dump(precision_metrics, file, indent=4)
    print("metrics.json file created successfully.")

if recommendations:
    with open("recommendations.json", "w", encoding="utf-8") as f:
        json.dump(recommendations, f, indent=4)
    print("recommendations.json file created successfully.")


# ######################## Export:
# # Step 1: Filter the DataFrame based on precision recommendations

# id_columns = pack_config.get('job', {}).get('id_columns', [])

# # For simplicity, let's assume that columns with a proportion score lower than 0.9 need attention
# columns_to_check = [item["scope"]["value"] for item in precision_metrics if item["key"] == "proportion_score" and float(item["value"]) < 0.9]

# # Filter the DataFrame for rows that don't meet the rounding criteria in the specified columns
# expected_precision = float(precision_metrics[1]["value"])
# rows_with_rounding_issues = df[df[columns_to_check].applymap(lambda x: isinstance(x, float) and (len(str(x).split(".")[1]) if '.' in str(x) else 0) != expected_precision)]

# # Check if there are rows with rounding issues
# if rows_with_rounding_issues.empty:
#     print("No rounding issues found. No report will be generated.")
# else:
#     # If there are rows with rounding issues, proceed with sorting and exporting
#     rows_with_rounding_issues = rows_with_rounding_issues.sort_values(by=columns_to_check)

#     # Step 3: Set index or create 'index' column for the Excel export
#     if id_columns:
#         # Ensure all id_columns are in the DataFrame columns
#         valid_id_columns = [col for col in id_columns if col in rows_with_rounding_issues.columns]
#         if not valid_id_columns:
#             print("None of the specified 'id_columns' are in the DataFrame. Using default index.")
#             rows_with_rounding_issues = rows_with_rounding_issues.reset_index(drop=True)
#         else:
#             rows_with_rounding_issues = rows_with_rounding_issues.set_index(valid_id_columns)
#     else:
#         # If 'id_columns' is not provided or is empty, create an 'index' column with the original DataFrame's index
#         rows_with_rounding_issues = rows_with_rounding_issues.reset_index()

#     # Continue with the export process
#     if source_config['type'] == 'file':
#         source_file_dir = os.path.dirname(source_config['config']['path'])
#         current_date = datetime.now().strftime("%Y%m%d")
#         report_file_path = os.path.join(source_file_dir, f'rounding_issues_report_{source_config["name"]}_{current_date}.xlsx')

#         # Export rows with rounding issues to an Excel file
#         rows_with_rounding_issues.to_excel(report_file_path, index=False)  # Set index=False as 'original_index' is now a column
#         print(f"Rows with rounding issues have been exported to {report_file_path}")
