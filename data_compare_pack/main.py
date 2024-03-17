from qalita_core.pack import Pack
import re
import datacompy
from datetime import datetime
import os

pack = Pack()
pack.load_data("source")
pack.load_data("target")


# Checking if the columns exist in the DataFrames
compare_col_list = pack.pack_config["job"].get("compare_col_list", [])
id_columns = pack.pack_config["job"].get("id_columns", [])
abs_tol = pack.pack_config["job"].get("abs_tol", 0.0001)
rel_tol = pack.pack_config["job"].get("rel_tol", 0)

# Create an intersection of source and target columns if compare_col_list is empty
if compare_col_list == []:
    compare_col_list = list(
        set(pack.df_source.columns).intersection(set(pack.df_target.columns))
    )

# Include only the relevant columns and ensure they exist
missing_in_source = [
    col for col in compare_col_list if col not in pack.df_source.columns
]
missing_in_target = [
    col for col in compare_col_list if col not in pack.df_target.columns
]

if missing_in_source:
    raise ValueError(f"Columns missing in source: {missing_in_source}")
if missing_in_target:
    raise ValueError(f"Columns missing in target: {missing_in_target}")

# Combine compare_col_list and id_columns while removing duplicates
combined_columns_list = list(dict.fromkeys(compare_col_list + id_columns))

if len(id_columns) == 0:
    id_columns = compare_col_list

# Creating subsets for source and target data with no repeated columns
df_source_subset = pack.df_source[combined_columns_list]
df_target_subset = pack.df_target[combined_columns_list]

############################ Comparison using datacompy
compare = datacompy.Compare(
    df_source_subset,
    df_target_subset,
    join_columns=id_columns,  # ID COLUMN
    abs_tol=abs_tol,  # Absolute tolerance
    rel_tol=rel_tol,  # Relative tolerance
    df1_name=pack.source_config["name"],
    df2_name=pack.target_config["name"],
)

comparison_report = compare.report(sample_count=10, column_count=10)

# Exporting comparison metrics :

pack.metrics.data.append(
    {
        "key": "dataframe_summary_number_columns_" + pack.source_config["name"],
        "value": compare.df1.shape[1],
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)
pack.metrics.data.append(
    {
        "key": "dataframe_summary_number_columns_" + pack.target_config["name"],
        "value": compare.df2.shape[1],
        "scope": {"perimeter": "dataset", "value": pack.target_config["name"]},
    }
)
pack.metrics.data.append(
    {
        "key": "dataframe_summary_number_rows_" + pack.source_config["name"],
        "value": compare.df1.shape[0],
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)
pack.metrics.data.append(
    {
        "key": "dataframe_summary_number_rows_" + pack.target_config["name"],
        "value": compare.df2.shape[0],
        "scope": {"perimeter": "dataset", "value": pack.target_config["name"]},
    }
)


# Optionally, save the report to an HTML file
with open("comparison_report.txt", "w") as f:
    f.write(comparison_report)

############################ Extracting metrics from the report
# Dictionary to hold the extracted data
extracted_data = {}

# Define patterns for the parts you want to extract
patterns = {
    "column_summary": r"Column Summary\s+-+\s+([\s\S]+?)\n\n",
    "row_summary": r"Row Summary\s+-+\s+([\s\S]+?)\n\n",
    "column_comparison": r"Column Comparison\s+-+\s+([\s\S]+?)\n\n",
}

# Extract the data using the patterns
for key, pattern in patterns.items():
    match = re.search(pattern, comparison_report, re.DOTALL)
    if match:
        section_content = match.group(1)
        # Extract key-value pairs
        extracted_data[key] = dict(re.findall(r"([^\n:]+):\s*(\d+)", section_content))

# Convert extracted data to metrics
for section, data in extracted_data.items():
    for key, value in data.items():
        metric = {
            "key": f"{section}_{key.lower().replace(' ', '_')}",
            "value": value,
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
        pack.metrics.data.append(metric)

############################ Computing the matching score
# Initialize the dictionary to hold the values
metrics_values = {
    "Number of rows in common": 0,
    "Number of rows in Target but not in Source": 0,
    "Number of rows in Source but not in Target": 0,
}

source_name = pack.source_config["name"].lower().replace(" ", "_")
target_name = pack.target_config["name"].lower().replace(" ", "_")

# Base keys to match (ignoring specific dataset names)
base_keys = {
    "row_summary_number_of_rows_in_common": "Number of rows in common",
    f"row_summary_number_of_rows_in_{source_name}_but_not_in_{target_name}": "Number of rows in Target but not in Source",
    f"row_summary_number_of_rows_in_{target_name}_but_not_in_{source_name}": "Number of rows in Source but not in Target",
}

# Iterate over the metrics and extract the values
for metric in pack.metrics.data:
    for base_key, value_key in base_keys.items():
        # Check if the full_key is in the metric's key
        if base_key in metric["key"]:
            metrics_values[value_key] = int(metric["value"])
            print(f"Found {value_key} with value {metric['value']}")
            break  # Exit the inner loop if a match is found

# Extract the required values
num_rows_in_common = metrics_values["Number of rows in common"]
num_rows_in_target_not_in_source = metrics_values[
    "Number of rows in Target but not in Source"
]
num_rows_in_source_not_in_target = metrics_values[
    "Number of rows in Source but not in Target"
]

df_all_mismatch = compare.all_mismatch(ignore_matching_cols=True)

# Ensure the denominator is not zero to avoid division by zero error
total_target_rows = len(pack.df_target)
print(f"Total rows in target: {total_target_rows}")
if total_target_rows == 0:
    print("Cannot compute the score as the total number of rows in target is zero.")
else:
    num_mismatches = len(df_all_mismatch)
    if num_mismatches == 0:
        # If there are no mismatches, the score is 1 (100% match)
        score = 1.0
    else:
        # Calculate score as a ratio of matched rows to total rows in target dataframe
        score = max(0, 1 - (num_mismatches / total_target_rows))

    print(f"Matching score: {score}")

    # Append the score to the metrics
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(score, 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )

# Compute Precision and Recall with the available variables
if total_target_rows == 0:
    precision = 0  # Avoid division by zero; no rows to match in target makes precision undefined, considered as 0
else:
    precision = num_rows_in_common / total_target_rows

total_source_rows = len(pack.df_source)
if total_source_rows == 0:
    recall = 0  # Similarly, avoid division by zero; no rows in source makes recall undefined, considered as 0
else:
    recall = num_rows_in_common / total_source_rows

print(f"Precision: {precision}")
print(f"Recall: {recall}")

# Calculate the F1 score, which is the harmonic mean of precision and recall
if precision + recall == 0:
    f1_score = 0  # Avoid division by zero; if both precision and recall are 0, F1 is undefined, considered as 0
else:
    f1_score = 2 * (precision * recall) / (precision + recall)

print(f"F1 Score: {f1_score}")

# Append the precision, recall, and F1 score to the metrics
pack.metrics.data.extend(
    [
        {
            "key": "precision",
            "value": str(round(precision, 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
        {
            "key": "recall",
            "value": str(round(recall, 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
        {
            "key": "f1_score",
            "value": str(round(f1_score, 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
    ]
)

# Extracting column labels
columnLabels = df_all_mismatch.columns.tolist()

# Dictionary to map the old suffix to the new one
suffix_mapping = {"_df1": "_source", "_df2": "_target"}

# Revise the loop to correctly process replacement without duplication
new_columnLabels = [
    (
        col
        if not any(col.endswith(suffix) for suffix in suffix_mapping.keys())
        else next(
            col.replace(suffix, replacement)
            for suffix, replacement in suffix_mapping.items()
            if col.endswith(suffix)
        )
    )
    for col in columnLabels
]

# Assuming `df_all_mismatch` is your DataFrame, rename its columns with the new labels
df_all_mismatch.columns = new_columnLabels

# Since you've updated column names, you don't need to change the way you convert the DataFrame
data_formatted = [
    [{"value": row[col]} for col in df_all_mismatch.columns]
    for index, row in df_all_mismatch.iterrows()
]

# The formatted data structure, now with renamed labels
format_structure = {
    "columnLabels": new_columnLabels,  # Use the new column labels
    "data": data_formatted,
}

# Append the precision, recall, and F1 score to the metrics
pack.metrics.data.extend(
    [
        {
            "key": "recommendation_levels_mismatches",
            "value": {"info": "0", "warning": "0.5", "high": "0.8"},
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
        {
            "key": "check_column",
            "value": [combined_columns_list],
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
        {
            "key": "mismatches_table",
            "value": format_structure,
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
    ]
)

pack.metrics.save()

######################## Export:
# Check if there are any mismatches

if df_all_mismatch.empty:
    print("No mismatches found. No report will be generated.")
else:
    if pack.source_config["type"] == "file":
        source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(
            source_file_dir,
            f'{current_date}_data_compare_report_{pack.source_config["name"]}.xlsx',
        )

        # Export mismatches rows to an Excel file
        df_all_mismatch.to_excel(
            report_file_path, index=False
        )  # Set index=False as 'original_index' is now a column
        print(f"mismatches rows have been exported to {report_file_path}")
