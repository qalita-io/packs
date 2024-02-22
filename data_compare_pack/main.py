from qalita_core.pack import Pack
import datacompy

pack = Pack()
pack.load_data("source")
pack.load_data("target")

# Checking if the columns exist in the DataFrames
required_columns = pack.pack_config["job"]["col_list"]
missing_in_source = [col for col in required_columns if col not in pack.df_source.columns]
missing_in_target = [col for col in required_columns if col not in pack.df_target.columns]

if missing_in_source:
    raise ValueError(f"Columns missing in source: {missing_in_source}")
if missing_in_target:
    raise ValueError(f"Columns missing in target: {missing_in_target}")

if missing_in_source or missing_in_target:
    print("Comparison not performed due to missing columns.")
    raise

# If no columns are missing, proceed with the comparison

############################ Comparison using datacompy
compare = datacompy.Compare(
    pack.df_source,
    pack.df_target,
    join_columns=required_columns,  # Columns to join on
    abs_tol=0,  # Absolute tolerance
    rel_tol=0,  # Relative tolerance
    df1_name=pack.source_config["name"],
    df2_name=pack.target_config["name"],
)

comparison_report = compare.report(sample_count=10, column_count=10)

# Optionally, save the report to an HTML file
with open("comparison_report.txt", "w") as f:
    f.write(comparison_report)

############################ Extracting metrics from the report
# Dictionary to hold the extracted data
extracted_data = {}

# Define patterns for the parts you want to extract
patterns = {
    "dataframe_summary": r"DataFrame Summary\s+-+\s+([\s\S]+?)\n\n",
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
        extracted_data[key] = dict(
            re.findall(r"([^\n:]+):\s*(\d+)", section_content)
        )

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

# Ensure the denominator is not zero to avoid division by zero error
total_rows_in_target = num_rows_in_common + num_rows_in_target_not_in_source
print(f"Total rows in target: {total_rows_in_target}")
if total_rows_in_target == 0:
    print("Cannot compute the score as the total number of rows in target is zero.")
else:
    score = num_rows_in_common / total_rows_in_target
    print(f"Matching score: {score}")

    # Append the score to the metrics
    pack.metrics.data.append(
        {
            "key": "score",
            "value": score,
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )

pack.metrics.save()
