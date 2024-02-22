import json
import pandas as pd
from ydata_profiling import ProfileReport
from qalita_core.pack import Pack

pack = Pack()
pack.load_data("source")
df_dict = pack.df_source

########################### Profiling and Aggregating Results

# Ensure that data is loaded
if not df_dict:
    raise ValueError("No data loaded from the source.")

# Iterate over each dataset and create profile reports
for dataset_name, df in df_dict.items():
    # Determine the appropriate name for 'dataset' in 'scope'
    dataset_scope_name = (
        dataset_name
        if pack.source_config["type"] == "database"
        else pack.source_config["name"]
    )

    print(f"Generating profile for {dataset_name}")

    # Run the profiling report
    profile = ProfileReport(
        df, minimal=True, title=f"Profiling Report for {dataset_name}"
    )

    # Save the report to HTML
    html_file_name = f"{dataset_name}_report.html"
    profile.to_file(html_file_name)

    # Save the report to JSON
    json_file_name = f"{dataset_name}_report.json"
    profile.to_file(json_file_name)

    try:
        with open(html_file_name, "r", encoding="utf-8") as f:
            tables = pd.read_html(f.read())
    except ValueError as e:
        print(f"No tables found in the HTML report: {e}")
        tables = [pd.DataFrame()]  # Create an empty DataFrame if no tables are found

    # Load the JSON file
    print(f"Load {dataset_name}_report.json")
    with open(f"{dataset_name}_report.json", "r", encoding="utf-8") as file:
        report = json.load(file)

    ############################ Schemas
    # Add entries for each variable
    for variable_name in report["variables"].keys():
        pack.schemas.data.append(
            {
                "key": "column",
                "value": variable_name,
                "scope": {
                    "perimeter": "column",
                    "value": variable_name,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_scope_name,
                    },
                },
            }
        )

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

if pack.source_config["type"] == "database":
    pack.schemas.data.append(
        {
            "key": "database",
            "value": pack.source_config["name"],
            "scope": {"perimeter": "database", "value": pack.source_config["name"]},
        }
    )

pack.schemas.save()