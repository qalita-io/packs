"""
Main file for pack
"""
import re
import json
import warnings
import pandas as pd
from ydata_profiling import ProfileReport
from opener import load_data

warnings.filterwarnings("ignore", category=DeprecationWarning)

def round_if_numeric(value, decimals=2):
    try:
        # Convert to a float and round
        rounded_value = round(float(value), decimals)
        # If the rounded value is an integer, convert it to an int
        if rounded_value.is_integer():
            return str(int(rounded_value))
        # Otherwise, format it as a string with two decimal places
        return "{:.2f}".format(rounded_value)
    except (ValueError, TypeError):
        # Return the original value if it's not a number
        return str(value)

# Function to extract percentage and determine level
def determine_level(content):
    """
    Function to extract percentage and determine level
    """
    # Find percentage value in the string
    match = re.search(r"(\d+(\.\d+)?)%", content)
    if match:
        percentage = float(match.group(1))
        # Determine level based on percentage
        if 0 <= percentage <= 70:
            return "info"
        elif 71 <= percentage <= 90:
            return "warning"
        elif 91 <= percentage <= 100:
            return "high"
    return "info"  # Default level if no percentage is found


# Denormalize a dictionary with nested dictionaries
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

# Load data using the opener.py logic
df = load_data(config)

# Run the profiling report
profile = ProfileReport(df, minimal=True, title="Profiling Report")
profile.to_file("report.html")
tables = pd.read_html("report.html")
profile.to_file("report.json")

############################ Metrics

# Calculate the completeness score for each column
completeness_scores = []
for col in df.columns:
    non_null_count = df[col].notnull().sum()
    total_count = len(df)
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

general_data = denormalize(report["table"])
new_format_data = []
for key, value in general_data.items():
    entry = {
        "key": key,
        "value": round_if_numeric(value),
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
            "value": round_if_numeric(attr_value),
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
        "value": str(round(
            (int(number_of_observations) - int(missing_cells))
            / int(number_of_observations), 2
        )),
        "scope": {"perimeter": "dataset", "value": config["name"]},
    },
    index=[0],
)

############################ Recommendations

alerts = tables[2]
alerts.columns = ["content", "type"]
# Set the scope perimeter as 'column'
alerts["scope"] = alerts["content"].str.split().str[0]

# Convert the scope to JSON
alerts["scope"] = alerts["scope"].apply(lambda x: {"perimeter": "column", "value": x})

# Apply the function to the 'content' column of the alerts DataFrame
alerts["level"] = alerts["content"].apply(determine_level)

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
