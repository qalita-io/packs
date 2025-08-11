from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level
from datetime import datetime
import os

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
pack = Pack()
if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

# Define uniqueness_columns if not specified in pack_config
if (
    "job" in pack.pack_config
    and "compute_uniqueness_columns" in pack.pack_config["job"]
    and len(pack.pack_config["job"]["compute_uniqueness_columns"]) > 0
):
    uniqueness_columns = pack.pack_config["job"]["compute_uniqueness_columns"]
else:
    uniqueness_columns = pack.df_source.columns

############################ Metrics

# Step 1: Filter the DataFrame based on the specified columns
print("Columns used for checking duplicates:", uniqueness_columns)
df_subset = pack.df_source[uniqueness_columns].copy()
duplicates = df_subset.duplicated()
total_rows = len(pack.df_source)

print("total rows "+str(total_rows))

# Step 2: Calculate the number of duplicate rows based on this subset
total_duplicates = duplicates.sum()

print("total duplicates "+str(total_duplicates))

# Calculate the scoped duplication score
duplication_score = round(total_duplicates / total_rows if total_rows > 0 else 0, 2)

# Invert the score for the scoped scenario
score = 1 - duplication_score

# Add the inverted duplication score to the metrics
pack.metrics.data.append(
    {
        "key": "score",
        "value": str(round(score, 2)),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)

# Create metric entries as DataFrames
pack.metrics.data.append(
    {
        "key": "duplicates",
        "value": int(total_duplicates),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)

# Check if scoped score is calculated and add its metrics
if (
    "job" in pack.pack_config
    and "compute_uniqueness_columns" in pack.pack_config["job"]
):
    pack.metrics.data.append(
        {
            "key": "duplicates",
            "value": int(total_duplicates),
            "scope": {
                "perimeter": "dataset",
                "value": ", ".join(uniqueness_columns),
            },
        }
    )

############################ Recommendations
if score < 0.9:

    recommendation = {
        "content": f"dataset '{pack.source_config['name']}' has a duplication rate of {duplication_score*100}%. on the scope {uniqueness_columns.to_list()} .",
        "type": "Duplicates",
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        "level": determine_recommendation_level(duplication_score),
    }
    pack.recommendations.data.append(recommendation)


pack.metrics.save()
pack.recommendations.save()

######################## Export:
# Step 1: Retrieve 'id_columns' from pack_config
id_columns = pack.pack_config.get("job", {}).get("id_columns", [])

# Step 2: Identify duplicated rows
duplicated_rows = pack.df_source[duplicates]

# Check if there are any duplicates
if duplicates.empty:
    print("No duplicates found. No report will be generated.")
else:
    # Step 3: Set index or create 'index' column for the Excel export
    if id_columns:
        # Ensure all id_columns are in the DataFrame columns
        valid_id_columns = [col for col in id_columns if col in duplicated_rows.columns]
        if not valid_id_columns:
            print(
                "None of the specified 'id_columns' are in the DataFrame. Using default index."
            )
            duplicated_rows = duplicated_rows.reset_index(drop=True)
        else:
            duplicated_rows = duplicated_rows.set_index(valid_id_columns)
    else:
        # If 'id_columns' is not provided or is empty, create an 'index' column with the original DataFrame's index
        duplicated_rows = duplicated_rows.reset_index()

    # Continue with the export process
    if pack.source_config["type"] == "file":
        source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(
            source_file_dir,
            f'{current_date}_duplicates_finder_report_{pack.source_config["name"]}.xlsx',
        )

        # Export duplicated rows to an Excel file
        duplicated_rows.to_excel(
            report_file_path, index=False
        )  # Set index=False as 'original_index' is now a column
        print(f"Duplicated rows have been exported to {report_file_path}")
