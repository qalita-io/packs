import pandas as pd
from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

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
    else:
        base = pack.source_config["name"]
        items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
else:
    items = [(pack.source_config["name"], _load_parquet_if_path(raw_df_source))]

for dataset_label, df_curr in items:
    float_columns = df_curr.select_dtypes(include=["float", "float64"]).columns
    if len(float_columns) == 0:
        print(f"[{dataset_label}] No float columns found. Skipping.")
        continue

    total_proportion_score = 0
    valid_columns_count = 0
    float_total_proportion_score = 0
    valid_points_count = 0

    for column in float_columns:
        column_data = df_curr[column].dropna()
        if column_data.empty:
            continue
        valid_data_points = len(column_data)
        decimals_count = column_data.apply(lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0)
        max_decimals = decimals_count.max()
        most_common_decimals_series = decimals_count.mode()
        if most_common_decimals_series.empty:
            proportion_score = 0
        else:
            most_common_decimals = most_common_decimals_series[0]
            proportion_score = decimals_count[decimals_count == most_common_decimals].count() / valid_data_points

        total_proportion_score += proportion_score
        valid_columns_count += 1
        float_total_proportion_score += proportion_score * valid_data_points
        valid_points_count += valid_data_points

        if max_decimals > 0:
            pack.metrics.data.append(
                {"key": "decimal_precision", "value": str(max_decimals), "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}}}
            )
        pack.metrics.data.append(
            {"key": "proportion_score", "value": str(round(proportion_score, 2)), "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}}}
        )

    mean_proportion_score = total_proportion_score / valid_columns_count if valid_columns_count > 0 else 0
    float_mean_proportion_score = float_total_proportion_score / valid_points_count if valid_points_count > 0 else 0
    pack.metrics.data.append(
        {"key": "float_score", "value": str(round(float_mean_proportion_score, 2)), "scope": {"perimeter": "dataset", "value": dataset_label}}
    )
    pack.metrics.data.append(
        {"key": "score", "value": str(round(mean_proportion_score, 2)), "scope": {"perimeter": "dataset", "value": dataset_label}}
    )

    for column in df_curr.columns:
        for item in pack.metrics.data:
            scope = item.get("scope", {})
            parent = scope.get("parent_scope", {})
            if parent.get("value") != dataset_label:
                continue
            if scope.get("value") == column and item["key"] == "proportion_score":
                proportion_score = float(item["value"])
                if proportion_score < 0.9:
                    pack.recommendations.data.append(
                        {
                            "content": f"Column '{column}' has {(1-proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                            "type": "Unevenly Rounded Data",
                            "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                            "level": determine_recommendation_level(1 - proportion_score),
                        }
                    )

    if mean_proportion_score < 0.9:
        pack.recommendations.data.append(
            {
                "content": f"The dataset '{dataset_label}' has {(1-mean_proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                "type": "Unevenly Rounded Data",
                "scope": {"perimeter": "dataset", "value": dataset_label},
                "level": determine_recommendation_level(1 - mean_proportion_score),
            }
        )

pack.metrics.save()
pack.recommendations.save()
