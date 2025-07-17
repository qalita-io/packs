import json
import pandas as pd
from ydata_profiling import ProfileReport
from qalita_core.pack import Pack
from io import StringIO

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
pack = Pack()
if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("Pour une source de type 'database', il faut spécifier 'table_or_query' dans la config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

########################### Profiling and Aggregating Results
dataset_name = pack.source_config["name"]

print(f"Generating profile for {dataset_name}")

# Run the profiling report
profile = ProfileReport(pack.df_source, minimal=True, title=f"Profiling Report for {dataset_name}")

# Save the report to HTML
html_file_name = f"{dataset_name}_report.html"
profile.to_file(html_file_name)

# Save the report to JSON
json_file_name = f"{dataset_name}_report.json"
profile.to_file(json_file_name)

try:
    with open(html_file_name, "r", encoding="utf-8") as f:
        html_content = f.read()
        tables = pd.read_html(StringIO(html_content)) 
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
                    "value": dataset_name,
                },
            },
        }
    )

if pack.source_config["type"] == "database":
    pack.schemas.data.append(
        {
            "key": "dataset",
            "value": dataset_name,
            "scope": {
                "perimeter": "dataset",
                "value": dataset_name,
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
            "value": dataset_name,
            "scope": {"perimeter": "dataset", "value": dataset_name},
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