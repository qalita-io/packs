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
if isinstance(raw_df_source, list):
    if isinstance(configured, (list, tuple)) and len(configured) == len(raw_df_source):
        items = list(zip(list(configured), raw_df_source))
    else:
        base = pack.source_config["name"]
        items = [(f"{base}_{i+1}", df) for i, df in enumerate(raw_df_source)]
else:
    items = [(pack.source_config["name"], raw_df_source)]

############################ Metrics
epsilon = 1e-7  # Small number to prevent division by zero
outlier_threshold = pack.pack_config["job"].get("outlier_threshold", 0.5)
id_columns = pack.pack_config["job"].get("id_columns", [])

for dataset_label, df_curr in items:
    # Fill missing numeric with mean
    for column in df_curr.columns:
        if np.issubdtype(df_curr[column].dtype, np.number):
            df_curr[column] = df_curr[column].fillna(df_curr[column].mean())

    # Drop still-NaN columns
    columns_with_nan = [column for column in df_curr.columns if df_curr[column].isnull().any()]
    df_curr = df_curr.drop(columns=columns_with_nan)
    if df_curr.isnull().values.any():
        raise ValueError("Unexpected NaN values are still present after cleaning.")

    # Univariate
    univariate_outliers = {}
    for column in [col for col in df_curr.columns if col not in id_columns]:
        if np.issubdtype(df_curr[column].dtype, np.number):
            clf = KNN()
            clf.fit(df_curr[[column]])
            scores = clf.decision_function(df_curr[[column]])
            inlier_score = 1 - scores / (scores.max() + epsilon)
            pack.metrics.data.append(
                {
                    "key": "normality_score",
                    "value": round(inlier_score.mean().item(), 2),
                    "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                }
            )
            outliers = df_curr[[column]][inlier_score < outlier_threshold].copy()
            univariate_outliers[column] = outliers
            outlier_count = len(outliers)
            pack.metrics.data.append(
                {
                    "key": "outliers",
                    "value": outlier_count,
                    "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                }
            )
            if outlier_count > 0:
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
        pack.recommendations.data.append(
            {
                "content": f"The dataset '{dataset_label}' has a normality score of {dataset_normality_score*100}%.",
                "type": "Outliers",
                "scope": {"perimeter": "dataset", "value": dataset_label},
                "level": determine_recommendation_level(1 - dataset_normality_score),
            }
        )

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

    # Step 4: Save to Excel per dataset
    if pack.source_config["type"] == "file":
        source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
        current_date = datetime.now().strftime("%Y%m%d")
        excel_file_name = (
            f"{current_date}_outlier_detection_report_{dataset_label}.xlsx"
        )
        excel_file_path = os.path.join(source_file_dir, excel_file_name)
        with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
            all_univariate_outliers.to_excel(
                writer, sheet_name="Univariate Outliers", index=False
            )
            multivariate_outliers.to_excel(
                writer, sheet_name="Multivariate Outliers", index=False
            )
            all_outliers.to_excel(writer, sheet_name="All Outliers", index=False)
        print(f"Outliers report saved to {excel_file_path}")

# Save artifacts once after processing all datasets
pack.recommendations.save()
pack.metrics.save()
