import warnings
# Silence noisy pkg_resources deprecation warning triggered by fs namespace package
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r"pkg_resources is deprecated as an API",
)
import re
import os
import pandas as pd
import datacompy
from datetime import datetime
from qalita_core.pack import Pack

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

if pack.target_config.get("type") == "database":
    table_or_query = pack.target_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("Pour une cible de type 'database', il faut spécifier 'table_or_query' dans la config.")
    pack.load_data("target", table_or_query=table_or_query)
else:
    pack.load_data("target")


# Checking if the columns exist in the DataFrames
compare_col_list = pack.pack_config["job"].get("compare_col_list", [])
id_columns = pack.pack_config["job"].get("id_columns", [])
abs_tol = pack.pack_config["job"].get("abs_tol", 0.0001)
rel_tol = pack.pack_config["job"].get("rel_tol", 0)

def _load_parquet_if_path(obj):
    try:
        if isinstance(obj, str) and obj.lower().endswith((".parquet", ".pq")):
            return pd.read_parquet(obj, engine="pyarrow")
    except Exception:
        pass
    return obj

raw_source = pack.df_source
raw_target = pack.df_target

# Normalize to list of (label, df)
def to_items(raw_df, conf, default_name):
    if isinstance(raw_df, list):
        loaded = [_load_parquet_if_path(x) for x in raw_df]
        names = conf.get("config", {}).get("table_or_query")
        if isinstance(names, (list, tuple)) and len(names) == len(loaded):
            return list(zip(list(names), loaded))
        else:
            return [(f"{default_name}_{i+1}", df) for i, df in enumerate(loaded)]
    else:
        return [(default_name, _load_parquet_if_path(raw_df))]

source_items = to_items(raw_source, pack.source_config, pack.source_config["name"])
target_items = to_items(raw_target, pack.target_config, pack.target_config["name"])

# Compare pairwise by index if both are lists, otherwise single pair
pairings = []
if len(source_items) == 1 and len(target_items) == 1:
    pairings = [(source_items[0], target_items[0])]  # ((s_label, s_df), (t_label, t_df))
elif len(source_items) == len(target_items):
    pairings = list(zip(source_items, target_items))
else:
    # Fallback: compare first of each and warn
    print("Source/Target tables count mismatch; comparing first dataset of each.")
    pairings = [(source_items[0], target_items[0])]

for (s_label, s_df), (t_label, t_df) in pairings:
    s_cols = set(s_df.columns)
    t_cols = set(t_df.columns)
    use_cols = compare_col_list or list(s_cols.intersection(t_cols))
    missing_in_source = [col for col in use_cols if col not in s_df.columns]
    missing_in_target = [col for col in use_cols if col not in t_df.columns]
    if missing_in_source:
        raise ValueError(f"Columns missing in source {s_label}: {missing_in_source}")
    if missing_in_target:
        raise ValueError(f"Columns missing in target {t_label}: {missing_in_target}")

    combined_columns_list = list(dict.fromkeys(use_cols + id_columns))
    if len(id_columns) == 0:
        id_columns = use_cols

    df_source_subset = s_df[combined_columns_list]
    df_target_subset = t_df[combined_columns_list]

############################ Comparison using datacompy
    compare = datacompy.Compare(
        df_source_subset,
        df_target_subset,
        join_columns=id_columns,
        abs_tol=abs_tol,
        rel_tol=rel_tol,
        df1_name=s_label,
        df2_name=t_label,
    )

    comparison_report = compare.report(sample_count=10, column_count=10)

# Exporting comparison metrics :

    pack.metrics.data.append(
        {"key": "dataframe_summary_number_columns_" + s_label, "value": compare.df1.shape[1], "scope": {"perimeter": "dataset", "value": s_label}}
    )
    pack.metrics.data.append(
        {"key": "dataframe_summary_number_columns_" + t_label, "value": compare.df2.shape[1], "scope": {"perimeter": "dataset", "value": t_label}}
    )
    pack.metrics.data.append(
        {"key": "dataframe_summary_number_rows_" + s_label, "value": compare.df1.shape[0], "scope": {"perimeter": "dataset", "value": s_label}}
    )
    pack.metrics.data.append(
        {"key": "dataframe_summary_number_rows_" + t_label, "value": compare.df2.shape[0], "scope": {"perimeter": "dataset", "value": t_label}}
    )


# Optionally, save the report to an HTML file
    with open(f"comparison_report_{s_label}_vs_{t_label}.txt", "w") as f:
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
            extracted_data[key] = dict(re.findall(r"([^\n:]+):\s*(\d+)", section_content))

    # Convert extracted data to metrics
    for section, data in extracted_data.items():
        for key, value in data.items():
            pack.metrics.data.append(
                {
                    "key": f"{section}_{key.lower().replace(' ', '_')}",
                    "value": value,
                    "scope": {"perimeter": "dataset", "value": s_label},
                }
            )

    ############################ Computing the matching score
    metrics_values = {
        "Number of rows in common": 0,
        "Number of rows in Target but not in Source": 0,
        "Number of rows in Source but not in Target": 0,
    }

    source_name = s_label.lower().replace(" ", "_")
    target_name = t_label.lower().replace(" ", "_")

    base_keys = {
        "row_summary_number_of_rows_in_common": "Number of rows in common",
        f"row_summary_number_of_rows_in_{source_name}_but_not_in_{target_name}": "Number of rows in Target but not in Source",
        f"row_summary_number_of_rows_in_{target_name}_but_not_in_{source_name}": "Number of rows in Source but not in Target",
    }

    for metric in pack.metrics.data:
        for base_key, value_key in base_keys.items():
            if base_key in metric["key"] and metric.get("scope", {}).get("value") == s_label:
                metrics_values[value_key] = int(metric["value"])
                print(f"Found {value_key} with value {metric['value']}")
                break

    num_rows_in_common = metrics_values["Number of rows in common"]
    num_rows_in_target_not_in_source = metrics_values["Number of rows in Target but not in Source"]
    num_rows_in_source_not_in_target = metrics_values["Number of rows in Source but not in Target"]

    df_all_mismatch = compare.all_mismatch(ignore_matching_cols=True)

    total_target_rows = len(t_df)
    print(f"[{s_label} vs {t_label}] Total rows in target: {total_target_rows}")
    if total_target_rows == 0:
        print("Cannot compute the score as the total number of rows in target is zero.")
    else:
        num_mismatches = len(df_all_mismatch)
        if num_mismatches == 0:
            score = 1.0
        else:
            score = max(0, 1 - (num_mismatches / total_target_rows))
        print(f"Matching score: {score}")
        pack.metrics.data.append(
            {"key": "score", "value": str(round(score, 2)), "scope": {"perimeter": "dataset", "value": s_label}}
        )

    if total_target_rows == 0:
        precision = 0
    else:
        precision = num_rows_in_common / total_target_rows

    total_source_rows = len(s_df)
    if total_source_rows == 0:
        recall = 0
    else:
        recall = num_rows_in_common / total_source_rows

    print(f"Precision: {precision}")
    print(f"Recall: {recall}")

    if precision + recall == 0:
        f1_score = 0
    else:
        f1_score = 2 * (precision * recall) / (precision + recall)

    print(f"F1 Score: {f1_score}")

    pack.metrics.data.extend(
        [
            {"key": "precision", "value": str(round(precision, 2)), "scope": {"perimeter": "dataset", "value": s_label}},
            {"key": "recall", "value": str(round(recall, 2)), "scope": {"perimeter": "dataset", "value": s_label}},
            {"key": "f1_score", "value": str(round(f1_score, 2)), "scope": {"perimeter": "dataset", "value": s_label}},
        ]
    )

    columnLabels = df_all_mismatch.columns.tolist()
    suffix_mapping = {"_df1": "_source", "_df2": "_target"}
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
    df_all_mismatch.columns = new_columnLabels
    data_formatted = [
        [{"value": row[col]} for col in df_all_mismatch.columns]
        for index, row in df_all_mismatch.iterrows()
    ]
    format_structure = {"columnLabels": new_columnLabels, "data": data_formatted}
    pack.metrics.data.extend(
        [
            {"key": "recommendation_levels_mismatches", "value": {"info": "0", "warning": "0.5", "high": "0.8"}, "scope": {"perimeter": "dataset", "value": s_label}},
            {"key": "mismatches_table", "value": format_structure, "scope": {"perimeter": "dataset", "value": s_label}},
        ]
    )

    ######################## Export per pairing
    if not df_all_mismatch.empty and pack.source_config["type"] == "file":
        source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(
            source_file_dir,
            f"{current_date}_data_compare_report_{s_label}_vs_{t_label}.xlsx",
        )
        df_all_mismatch.to_excel(report_file_path, index=False)
        print(f"mismatches rows have been exported to {report_file_path}")

# Save metrics once after processing all pairings
pack.metrics.save()
