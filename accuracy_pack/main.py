import json
import utils

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

############################ Writing Metrics and Recommendations to Files

if precision_metrics is not None:
    with open("metrics.json", "w") as file:
        json.dump(precision_metrics, file, indent=4)
    print("metrics.json file created successfully.")

if recommendations:
    with open("recommendations.json", "w", encoding="utf-8") as f:
        json.dump(recommendations, f, indent=4)
    print("recommendations.json file created successfully.")
