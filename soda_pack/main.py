from soda.scan import Scan
from qalita_core.pack import Pack
from qalita_core.utils import (
    determine_recommendation_level,
    replace_whitespaces_with_underscores,
)

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

# Dictionary to hold the association between slugified and original column names
df, column_name_association = replace_whitespaces_with_underscores(pack.df_source)

print("Slugified Columns :", df.columns)
print(
    "Association table between original col names and slugified ones :",
    column_name_association,
)

########################### Scan
scan = Scan()
scan.set_data_source_name(
    pack.source_config["name"].replace(" ", "_").replace("-", "_")
)
scan.add_pandas_dataframe(
    data_source_name=pack.source_config["name"].replace(" ", "_").replace("-", "_"),
    dataset_name=pack.source_config["name"].replace(" ", "_").replace("-", "_"),
    pandas_df=df,
)

# Add check YAML files
scan.add_sodacl_yaml_files("checks.yaml")

# Execute the scan
scan.execute()

results = scan.get_scan_results()
checks = results["checks"]
############################ Metrics

# Reformat the checks object to the desired format
reformatted_checks_metrics = []
for check in results["metrics"]:
    # Parsing the identity field
    identity_parts = check["identity"].split("-")
    # Skipping the first two values
    source_column = identity_parts[3] if len(identity_parts) > 3 else None
    previous_value = identity_parts[2] if len(identity_parts) > 2 else None
    # Determining the scope
    if source_column == check["metricName"]:
        scope = {"perimeter": "dataset", "value": pack.source_config["name"]}
    else:
        original_column_name = column_name_association.get(source_column)
        scope = {
            "perimeter": "column",
            "value": original_column_name,
            "parent_scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        }

    # Formatting to the desired structure
    reformatted_check = {
        "key": check["metricName"],
        "value": check["value"],
        "scope": scope,
    }

    reformatted_checks_metrics.append(reformatted_check)

metrics = reformatted_checks_metrics

# Initialize counters for pass and total checks
total_pass_count = 0
total_checks = len(checks)

# Count the number of passed checks
for check in checks:
    if check["outcome"] == "pass":
        total_pass_count += 1

# Calculate the score as the proportion of passed checks
dataset_score = total_pass_count / total_checks if total_checks > 0 else 0

# Print the results
print(f"Total Checks: {total_checks}")
print(f"Passed Checks: {total_pass_count}")
print(f"Score: {dataset_score:.2f} (Proportion of Passed Checks)")

# Data to be written to JSON
metrics.append(
    {
        "key": "score",
        "value": round(dataset_score, 2),
        "scope": {
            "perimeter": "dataset",
            "value": pack.source_config["name"],
            "parent_scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        },
    }
)

metrics.append(
    {
        "key": "check_passed",
        "value": total_pass_count,
        "scope": {
            "perimeter": "dataset",
            "value": pack.source_config["name"],
            "parent_scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        },
    }
)

metrics.append(
    {
        "key": "check_failed",
        "value": (total_checks - total_pass_count),
        "scope": {
            "perimeter": "dataset",
            "value": pack.source_config["name"],
            "parent_scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        },
    }
)

# Initialize dictionaries to store pass counts and total checks for each column
column_pass_count = {}
column_total_checks = {}

# Iterate over the checks, incrementing counts for each column
for check in checks:
    # Use 'dataset' as the default if no column specified or if column is None
    column = check.get("column") or "dataset"
    column_pass_count[column] = column_pass_count.get(column, 0) + (
        1 if check["outcome"] == "pass" else 0
    )
    column_total_checks[column] = column_total_checks.get(column, 0) + 1

# Calculate the score for each column and append to the metrics
for column, total in column_total_checks.items():
    pass_count = column_pass_count.get(column, 0)
    score = pass_count / total if total > 0 else 0
    # Remove quotes if present in the column name
    column_name = (
        column.replace('"', "") if column != "dataset" else pack.source_config["name"]
    )
    original_column_name = column_name_association.get(column_name, column_name)

    metrics.append(
        {
            "key": f"check_completion_score",
            "value": round(score if column != "dataset" else dataset_score, 2),
            "scope": {
                "perimeter": "column" if column != "dataset" else "dataset",
                "value": original_column_name,
                "parent_scope": {
                    "perimeter": "dataset",
                    "value": pack.source_config["name"],
                },
            },
        }
    )

################# RECOMMENDATIONS #################
# Generate recommendations if the number of format is significant
recommendations = []

# Calculate the score as the proportion of passed checks
score = total_pass_count / total_checks if total_checks > 0 else 0
if score is not None and score < 1:
    recommendation = {
        "content": f"The dataset '{pack.source_config['name']}' has PASSED {total_pass_count}/{total_checks} checks giving a score of {score*100}%.",
        "type": "Checks Failed",
        "scope": {
            "perimeter": "dataset",
            "value": pack.source_config["name"],
            "parent_scope": {
                "perimeter": "dataset",
                "value": pack.source_config["name"],
            },
        },
        "level": determine_recommendation_level(score),
    }
    recommendations.append(recommendation)

for check in checks:
    if check["outcome"] != "pass":
        # Create a recommendation
        if check["column"] is not None:

            original_column_name = column_name_association.get(check["column"])

            recommendation = {
                "content": check["definition"],
                "type": check["name"],
                "scope": {
                    "perimeter": "column",
                    "value": original_column_name,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": pack.source_config["name"],
                    },
                },
                "level": "high",
            }
        else:
            recommendation = {
                "content": check["definition"],
                "type": "Checks Failed",
                "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
                "level": "high",
            }

        recommendations.append(recommendation)

pack.recommendations.save()
pack.metrics.save()
