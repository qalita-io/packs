from qalita_core.pack import Pack
import pandas as pd
from qalita_core.utils import determine_recommendation_level
from datetime import datetime
import os
from qalita_core.aggregation import detect_chunked_from_items, DuplicateAggregator, normalize_and_dedupe_recommendations

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

############################ Support single or multiple datasets
raw_df_source = pack.df_source
configured = pack.source_config.get("config", {}).get("table_or_query")

def _load_parquet_if_path(obj):
    try:
        if isinstance(obj, str) and obj.lower().endswith((".parquet", ".pq")):
            return pd.read_parquet(obj, engine="pyarrow")
    except Exception:
        pass
    return obj

if isinstance(raw_df_source, list):
    loaded = [_load_parquet_if_path(x) for x in raw_df_source]
    if isinstance(configured, (list, tuple)) and len(configured) == len(loaded):
        items = list(zip(list(configured), loaded))
        names_for_detect = [str(n) for n in configured]
    else:
        base = pack.source_config["name"]
        items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
        names_for_detect = [name for name, _ in items]
else:
    items = [(pack.source_config["name"], _load_parquet_if_path(raw_df_source))]
    names_for_detect = None

raw_items_list = raw_df_source if isinstance(raw_df_source, list) else [raw_df_source]
treat_chunks_as_one, auto_named, common_base_detected = detect_chunked_from_items(
    raw_items_list, names_for_detect, pack.source_config["name"]
)

# Process: either per dataset or aggregate across chunks into one scope
if treat_chunks_as_one:
    if (
        "job" in pack.pack_config
        and "compute_uniqueness_columns" in pack.pack_config["job"]
        and len(pack.pack_config["job"]["compute_uniqueness_columns"]) > 0
    ):
        uniqueness_columns = pack.pack_config["job"]["compute_uniqueness_columns"]
    else:
        uniqueness_columns = items[0][1].columns
    dup_agg = DuplicateAggregator(uniqueness_columns)
    for dataset_label, df_curr in items:
        dup_agg.add_df(df_curr)
    metrics, recommendations = dup_agg.finalize_metrics(pack.source_config["name"])
    pack.metrics.data.extend(metrics)
    try:
        score_item = next(m for m in metrics if m.get("key") == "score")
        score_val = float(score_item.get("value", 1.0))
        duplication_rate = 1.0 - score_val
        if score_val < 0.9:
            pack.recommendations.data.append(
                {
                    "content": f"dataset '{pack.source_config['name']}' has a duplication rate of {duplication_rate*100}% on the scope {list(uniqueness_columns)}.",
                    "type": "Duplicates",
                    "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
                    "level": determine_recommendation_level(duplication_rate),
                }
            )
    except StopIteration:
        pass
else:
    for dataset_label, df_curr in items:
        if (
            "job" in pack.pack_config
            and "compute_uniqueness_columns" in pack.pack_config["job"]
            and len(pack.pack_config["job"]["compute_uniqueness_columns"]) > 0
        ):
            uniqueness_columns = pack.pack_config["job"]["compute_uniqueness_columns"]
        else:
            uniqueness_columns = df_curr.columns

        print("Columns used for checking duplicates:", uniqueness_columns)
        df_subset = df_curr[uniqueness_columns].copy()
        duplicates = df_subset.duplicated()
        total_rows = len(df_curr)
        total_duplicates = duplicates.sum()

        print("[", dataset_label, "] total rows "+str(total_rows))
        print("[", dataset_label, "] total duplicates "+str(total_duplicates))

        duplication_score = round(total_duplicates / total_rows if total_rows > 0 else 0, 2)
        score = 1 - duplication_score

        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )
        pack.metrics.data.append(
            {
                "key": "duplicates",
                "value": int(total_duplicates),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )
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

        if score < 0.9:
            recommendation = {
                "content": f"dataset '{dataset_label}' has a duplication rate of {duplication_score*100}% on the scope {list(uniqueness_columns)}.",
                "type": "Duplicates",
                "scope": {"perimeter": "dataset", "value": dataset_label},
                "level": determine_recommendation_level(duplication_score),
            }
            pack.recommendations.data.append(recommendation)


pack.metrics.save()
pack.recommendations.save()

######################## Export:
# Step 1: Retrieve 'id_columns' from pack_config
id_columns = pack.pack_config.get("job", {}).get("id_columns", [])

# Step 2: Identify duplicated rows (for the first dataset only for export simplicity)
if isinstance(pack.df_source, list):
    export_df = items[0][1]
    export_uniqueness = (
        pack.pack_config.get("job", {}).get("compute_uniqueness_columns") or export_df.columns
    )
    export_duplicates = export_df[list(export_uniqueness)].duplicated()
    duplicated_rows = export_df[export_duplicates]
else:
    export_df = pack.df_source
    export_uniqueness = (
        pack.pack_config.get("job", {}).get("compute_uniqueness_columns") or export_df.columns
    )
    export_duplicates = export_df[list(export_uniqueness)].duplicated()
    duplicated_rows = export_df[export_duplicates]

# Check if there are any duplicates
if duplicated_rows.empty:
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
