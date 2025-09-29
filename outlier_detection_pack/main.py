from qalita_core.pack import Pack


# Define a function to determine recommendation level based on the proportion of outliers
def determine_recommendation_level(proportion_outliers):
    if proportion_outliers > 0.5:  # More than 50% of data are outliers
        return "high"
    elif proportion_outliers > 0.3:  # More than 30% of data are outliers
        return "warning"
    else:
        return "info"


import os
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import OneHotEncoder
from pyod.models.knn import KNN
from qalita_core.aggregation import (
    detect_chunked_from_items,
    OutlierAggregator,
    normalize_and_dedupe_recommendations,
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

############################ Metrics
epsilon = 1e-7  # Small number to prevent division by zero
outlier_threshold = pack.pack_config["job"].get("outlier_threshold", 0.5)
id_columns = pack.pack_config["job"].get("id_columns", [])

# Accumulateur partagé
agg = OutlierAggregator()

for dataset_label, df_curr in items:
    # Fill missing numeric with mean
    for column in df_curr.columns:
        if pd.api.types.is_numeric_dtype(df_curr[column]):
            df_curr[column] = df_curr[column].fillna(df_curr[column].mean())

    # Drop still-NaN columns
    columns_with_nan = [column for column in df_curr.columns if df_curr[column].isnull().any()]
    df_curr = df_curr.drop(columns=columns_with_nan)
    if df_curr.isnull().values.any():
        raise ValueError("Unexpected NaN values are still present after cleaning.")

    # Verify dataset has enough rows for KNN (n_neighbors < n_samples_fit)
    knn_default_neighbors = 5
    min_required_samples = knn_default_neighbors + 1
    if len(df_curr) < min_required_samples:
        raise ValueError(
            f"[{dataset_label}] Dataset too small for KNN: at least {min_required_samples} rows required (current: {len(df_curr)})."
        )

    # Univariate
    univariate_outliers = {}
    for column in [col for col in df_curr.columns if col not in id_columns]:
        if pd.api.types.is_numeric_dtype(df_curr[column]):
            clf = KNN()
            clf.fit(df_curr[[column]])
            scores = clf.decision_function(df_curr[[column]])
            inlier_score = 1 - scores / (scores.max() + epsilon)
            col_mean = float(inlier_score.mean().item())
            if treat_chunks_as_one:
                agg.add_column_stats(
                    column=column,
                    mean_normality=col_mean,
                    outlier_count=int((inlier_score < outlier_threshold).sum()),
                    rows=len(df_curr),
                )
            else:
                pack.metrics.data.append(
                    {
                        "key": "normality_score",
                        "value": round(col_mean, 2),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    }
                )
            outliers = df_curr[[column]][inlier_score < outlier_threshold].copy()
            univariate_outliers[column] = outliers
            outlier_count = len(outliers)
            if not treat_chunks_as_one:
                pack.metrics.data.append(
                    {
                        "key": "outliers",
                        "value": outlier_count,
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    }
                )
            if outlier_count > 0 and not treat_chunks_as_one:
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column}' has {outlier_count} outliers.",
                        "type": "Outliers",
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                        "level": determine_recommendation_level(outlier_count / len(df_curr[[column]])),
                    }
                )

    total_univariate_outliers = sum(len(outliers) for outliers in univariate_outliers.values())

    # Encode categoricals
    non_numeric_columns = df_curr.select_dtypes(exclude=[np.number]).columns
    encoder = OneHotEncoder(drop="if_binary")
    if len(non_numeric_columns) > 0:
        encoded_data = encoder.fit_transform(df_curr[non_numeric_columns])
        encoded_df = pd.DataFrame(encoded_data.toarray(), columns=encoder.get_feature_names_out())
        df_num = df_curr.drop(non_numeric_columns, axis=1)
        df_num = pd.concat([df_num, encoded_df.reset_index(drop=True)], axis=1)
    else:
        df_num = df_curr

    df_for_multivariate = df_num.drop(columns=[c for c in id_columns if c in df_num.columns])
    multivariate_outliers = pd.DataFrame()
    try:
        clf = KNN()
        clf.fit(df_for_multivariate)
        scores = clf.decision_function(df_for_multivariate)
        inlier_score = 1 - scores / (scores.max() + epsilon)
        multivariate_outliers = df_curr.loc[inlier_score < outlier_threshold].copy()
        if treat_chunks_as_one:
            agg.add_dataset_stats(
                mean_normality=float(inlier_score.mean().item()),
                rows=len(df_curr),
                multivariate_outliers_count=int(len(multivariate_outliers)),
            )
        else:
            pack.metrics.data.append(
                {
                    "key": "outliers",
                    "value": len(multivariate_outliers),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            pack.metrics.data.append(
                {
                    "key": "normality_score_dataset",
                    "value": round(inlier_score.mean().item(), 2),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
            pack.metrics.data.append(
                {
                    "key": "score",
                    "value": str(round(inlier_score.mean().item(), 2)),
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                }
            )
    except ValueError as e:
        print(f"[{dataset_label}] Error fitting the model: {e}")


    total_multivariate_outliers = len(multivariate_outliers)
    total_outliers_count = total_univariate_outliers
    if not treat_chunks_as_one:
        pack.metrics.data.append(
            {
                "key": "total_outliers_count",
                "value": total_outliers_count,
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )


    # Define a threshold for considering a data point as an outlier (per dataset loop)
    normality_threshold = pack.pack_config["job"]["normality_threshold"]

    # Univariate Outlier Recommendations per dataset
    if not treat_chunks_as_one:
        for item in [m for m in pack.metrics.data if m["key"] == "normality_score"]:
            scope = item.get("scope", {})
            parent = scope.get("parent_scope", {})
            if parent.get("value") != dataset_label:
                continue
            if item["value"] < normality_threshold:
                column_name = scope.get("value")
                pack.recommendations.data.append(
                    {
                        "content": f"Column '{column_name}' has a normality score of {item['value']*100}%.",
                        "type": "Outliers",
                        "scope": {"perimeter": "column", "value": column_name, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                        "level": determine_recommendation_level(1 - item["value"]),
                    }
                )

    # Multivariate Outlier Recommendation per dataset
    dataset_normality_score = next(
        (m["value"] for m in pack.metrics.data if m["key"] == "normality_score_dataset" and m["scope"].get("value") == dataset_label),
        None,
    )
    if dataset_normality_score is not None and dataset_normality_score < normality_threshold:
        if not treat_chunks_as_one:
            pack.recommendations.data.append(
                {
                    "content": f"The dataset '{dataset_label}' has a normality score of {dataset_normality_score*100}%.",
                    "type": "Outliers",
                    "scope": {"perimeter": "dataset", "value": dataset_label},
                    "level": determine_recommendation_level(1 - dataset_normality_score),
                }
            )

    if treat_chunks_as_one:
        # Accumulations d'exports conservées localement puis rassemblées en fin
        all_univariate_outliers = pd.DataFrame()
        for column, outliers in univariate_outliers.items():
            outliers_with_id = df_curr.loc[outliers.index, id_columns + [column]].copy()
            outliers_with_id["value"] = outliers_with_id[column]
            outliers_with_id["OutlierAttribute"] = column
            outliers_with_id["index"] = outliers_with_id.index
            outliers_with_id = outliers_with_id[id_columns + ["index", "OutlierAttribute", "value"]]
            all_univariate_outliers = pd.concat([all_univariate_outliers, outliers_with_id], ignore_index=True)

        all_univariate_outliers_simple = pd.DataFrame()
        for column, outliers in univariate_outliers.items():
            outliers_with_id = df_curr.loc[outliers.index, id_columns + [column]].copy()
            outliers_with_id["OutlierAttribute"] = column
            outliers_with_id["index"] = outliers_with_id.index
            all_univariate_outliers_simple = pd.concat([all_univariate_outliers_simple, outliers_with_id], ignore_index=True)

        if not hasattr(agg, "_exports_full"):
            agg._exports_full = []  # type: ignore[attr-defined]
            agg._exports_simple = []  # type: ignore[attr-defined]
            agg._exports_mv = []  # type: ignore[attr-defined]
        agg._exports_full.append(all_univariate_outliers)  # type: ignore[attr-defined]
        agg._exports_simple.append(all_univariate_outliers_simple)  # type: ignore[attr-defined]
        mv = multivariate_outliers.copy()
        mv["index"] = mv.index
        mv["OutlierAttribute"] = "Multivariate"
        mv.reset_index(drop=True, inplace=True)
        agg._exports_mv.append(mv)  # type: ignore[attr-defined]
    else:
        pack.recommendations.data.append(
            {
                "content": f"The dataset '{dataset_label}' has a total of {total_outliers_count} outliers. Check them in output file.",
                "type": "Outliers",
                "scope": {"perimeter": "dataset", "value": dataset_label},
                "level": determine_recommendation_level(total_outliers_count / max(1, len(df_curr))),
            }
        )

    ####################### Export per dataset
    # Step 1: Compile Univariate Outliers
    all_univariate_outliers = pd.DataFrame()
    for column, outliers in univariate_outliers.items():
        outliers_with_id = df_curr.loc[outliers.index, id_columns + [column]].copy()
        outliers_with_id["value"] = outliers_with_id[column]
        outliers_with_id["OutlierAttribute"] = column
        outliers_with_id["index"] = outliers_with_id.index
        outliers_with_id = outliers_with_id[
            id_columns + ["index", "OutlierAttribute", "value"]
        ]
        all_univariate_outliers = pd.concat(
            [all_univariate_outliers, outliers_with_id], ignore_index=True
        )

    all_univariate_outliers_simple = pd.DataFrame()
    for column, outliers in univariate_outliers.items():
        outliers_with_id = df_curr.loc[outliers.index, id_columns + [column]].copy()
        outliers_with_id["OutlierAttribute"] = column
        outliers_with_id["index"] = outliers_with_id.index
        all_univariate_outliers_simple = pd.concat(
            [all_univariate_outliers_simple, outliers_with_id], ignore_index=True
        )

    id_and_other_columns = (
        ["index"] + id_columns + ["OutlierAttribute", "value"]
    )
    all_univariate_outliers = all_univariate_outliers[id_and_other_columns]

    columnLabels = all_univariate_outliers.columns.tolist()
    data_formatted = [
        [{"value": row[col]} for col in all_univariate_outliers.columns]
        for index, row in all_univariate_outliers.iterrows()
    ]
    format_structure = {"columnLabels": columnLabels, "data": data_formatted}
    pack.metrics.data.append(
        {
            "key": "outliers_table",
            "value": format_structure,
            "scope": {"perimeter": "dataset", "value": dataset_label},
        }
    )

    multivariate_outliers["index"] = multivariate_outliers.index
    multivariate_outliers["OutlierAttribute"] = "Multivariate"
    multivariate_outliers.reset_index(drop=True, inplace=True)
    id_and_other_columns = (
        ["index"]
        + id_columns
        + ["OutlierAttribute"]
        + [
            col
            for col in multivariate_outliers.columns
            if col not in ["index"] + id_columns + ["OutlierAttribute"]
        ]
    )
    multivariate_outliers = multivariate_outliers[id_and_other_columns]
    all_outliers = pd.concat(
        [all_univariate_outliers_simple, multivariate_outliers], ignore_index=True
    )
    all_outliers = all_outliers[id_and_other_columns]

    # Step 4: Save to Excel per dataset (sauf si agrégation, alors après la boucle)
    if not treat_chunks_as_one:
        dest_dir = os.getcwd()
        os.makedirs(dest_dir, exist_ok=True)
        current_date = datetime.now().strftime("%Y%m%d")
        excel_file_name = (
            f"{current_date}_outlier_detection_report_{dataset_label}.xlsx"
        )
        excel_file_path = os.path.join(dest_dir, excel_file_name)
        with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
            all_univariate_outliers.to_excel(writer, sheet_name="Univariate Outliers", index=False)
            multivariate_outliers.to_excel(writer, sheet_name="Multivariate Outliers", index=False)
            all_outliers.to_excel(writer, sheet_name="All Outliers", index=False)
        print(f"Outliers report saved to {excel_file_path}")

# Post-traitement: si agrégation, construire métriques/recommandations/export sur un périmètre unique
if treat_chunks_as_one:
    root_name = pack.source_config["name"]
    # Finaliser via l'agrégateur partagé
    normality_threshold = pack.pack_config["job"]["normality_threshold"]
    metrics, recommendations = agg.finalize_metrics_and_recommendations(
        root_dataset_name=root_name, normality_threshold=normality_threshold
    )
    pack.metrics.data.extend(metrics)
    pack.recommendations.data.extend(recommendations)

    # Normaliser/dédupliquer les recommandations si besoin
    pack.recommendations.data = normalize_and_dedupe_recommendations(
        pack.recommendations.data, root_name
    )

    # Excel unique
    dest_dir = os.getcwd()
    os.makedirs(dest_dir, exist_ok=True)
    current_date = datetime.now().strftime("%Y%m%d")
    excel_file_name = f"{current_date}_outlier_detection_report_{root_name}.xlsx"
    excel_file_path = os.path.join(dest_dir, excel_file_name)
    exports_full = getattr(agg, "_exports_full", [])  # type: ignore[attr-defined]
    exports_simple = getattr(agg, "_exports_simple", [])  # type: ignore[attr-defined]
    exports_mv = getattr(agg, "_exports_mv", [])  # type: ignore[attr-defined]
    all_univariate_outliers = pd.concat(exports_full, ignore_index=True) if exports_full else pd.DataFrame()
    all_univariate_outliers_simple = pd.concat(exports_simple, ignore_index=True) if exports_simple else pd.DataFrame()
    all_multivariate = pd.concat(exports_mv, ignore_index=True) if exports_mv else pd.DataFrame()
    all_outliers = pd.concat([all_univariate_outliers_simple, all_multivariate], ignore_index=True)
    with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
        all_univariate_outliers.to_excel(writer, sheet_name="Univariate Outliers", index=False)
        all_multivariate.to_excel(writer, sheet_name="Multivariate Outliers", index=False)
        all_outliers.to_excel(writer, sheet_name="All Outliers", index=False)
    print(f"Outliers report saved to {excel_file_path}")

# Save artifacts once after processing all datasets
pack.recommendations.save()
pack.metrics.save()
