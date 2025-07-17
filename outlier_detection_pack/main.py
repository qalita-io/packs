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
        raise ValueError("Pour une source de type 'database', il faut spécifier 'table_or_query' dans la config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

# Fill missing values with mean
for column in pack.df_source.columns:
    if np.issubdtype(pack.df_source[column].dtype, np.number):
        pack.df_source[column] = pack.df_source[column].fillna(
            pack.df_source[column].mean()
        )

# Identify columns that still contain NaN values after filling
columns_with_nan = [
    column for column in pack.df_source.columns if pack.df_source[column].isnull().any()
]

# Remove columns that still contain NaN values
pack.df_source.drop(columns=columns_with_nan, inplace=True)

# Check if there are any NaN values left in the dataframe (optional, for sanity check)
if pack.df_source.isnull().values.any():
    raise ValueError(
        "Unexpected NaN values are still present in the dataframe after filling and dropping."
    )

############################ Metrics
epsilon = 1e-7  # Small number to prevent division by zero
outlier_threshold = pack.pack_config["job"].get("outlier_threshold", 0.5)
id_columns = pack.pack_config["job"].get("id_columns", [])

# Dictionary to store univariate outliers for each column
univariate_outliers = {}

# Exclude id_columns from Univariate Outlier Detection
for column in [col for col in pack.df_source.columns if col not in id_columns]:
    if np.issubdtype(
        pack.df_source[column].dtype, np.number
    ):  # Process only numeric columns
        clf = KNN()
        clf.fit(pack.df_source[[column]])  # Fit model to the column

        # Get the outlier scores
        scores = clf.decision_function(pack.df_source[[column]])

        # Calculate the inlier score (100 - outlier score)
        inlier_score = 1 - scores / (
            scores.max() + epsilon
        )  # Normalize and convert to percentage

        pack.metrics.data.append(
            {
                "key": f"normality_score",
                "value": round(
                    inlier_score.mean().item(), 2
                ),  # Get the average score for the column
                "scope": {"perimeter": "column", "value": column},
            }
        )

        # Identify outliers based on the inlier score and threshold
        outliers = pack.df_source[[column]][inlier_score < outlier_threshold].copy()
        univariate_outliers[column] = outliers

        outlier_count = len(outliers)
        pack.metrics.data.append(
            {
                "key": f"outliers",
                "value": outlier_count,
                "scope": {"perimeter": "column", "value": column},
            }
        )

        if outlier_count > 0:
            pack.recommendations.data.append(
                {
                    "content": f"Column '{column}' has {outlier_count} outliers.",
                    "type": "Outliers",
                    "scope": {"perimeter": "column", "value": column},
                    "level": determine_recommendation_level(
                        outlier_count / len(pack.df_source[[column]])
                    ),
                }
            )


total_univariate_outliers = sum(
    len(outliers) for outliers in univariate_outliers.values()
)

# Identify non-numeric columns
non_numeric_columns = pack.df_source.select_dtypes(exclude=[np.number]).columns

# Apply OneHotEncoder
encoder = OneHotEncoder(drop="if_binary")
encoded_data = encoder.fit_transform(pack.df_source[non_numeric_columns])

# If you want a dense array, use .toarray()
encoded_df = pd.DataFrame(
    encoded_data.toarray(), columns=encoder.get_feature_names_out()
)

# Drop non-numeric columns from original df and concatenate with encoded_df
df = pack.df_source.drop(non_numeric_columns, axis=1)
df = pd.concat([df, encoded_df.reset_index(drop=True)], axis=1)

# Exclude id_columns from df before Multivariate Outlier Detection
df_for_multivariate = df.drop(columns=id_columns)

# Multivariate Outlier Detection
multivariate_outliers = pd.DataFrame()
try:
    clf = KNN()
    clf.fit(df_for_multivariate)  # Fit model to the dataset excluding id_columns

    # Get the outlier scores
    scores = clf.decision_function(df_for_multivariate)

    inlier_score = 1 - scores / (
        scores.max() + epsilon
    )  # Normalize and convert to percentage

    # Identify multivariate outliers based on the inlier score and threshold
    multivariate_outliers = pack.df_source.loc[inlier_score < outlier_threshold].copy()

    pack.metrics.data.append(
        {
            "key": "outliers",
            "value": len(multivariate_outliers),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )
    pack.metrics.data.append(
        {
            "key": "normality_score_dataset",
            "value": round(inlier_score.mean().item(), 2),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )
    pack.metrics.data.append(
        {
            "key": "score",
            "value": str(round(inlier_score.mean().item(), 2)),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        }
    )
except ValueError as e:
    print(f"Error fitting the model: {e}")


total_multivariate_outliers = len(multivariate_outliers)
# total_outliers_count = total_univariate_outliers + total_multivariate_outliers
total_outliers_count = total_univariate_outliers
pack.metrics.data.append(
    {
        "key": "total_outliers_count",
        "value": total_outliers_count,
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)


# Define a threshold for considering a data point as an outlier
normality_threshold = pack.pack_config["job"][
    "normality_threshold"
]  # Ensure this exists in your pack_conf.json

# Univariate Outlier Recommendations
for item in pack.metrics.data:
    if (
        item["key"].startswith("normality_score")
        and "column" in item["scope"]["perimeter"]
    ):
        # Check if the normality score is below the threshold
        if item["value"] < normality_threshold:
            column_name = item["scope"]["value"]
            recommendation = {
                "content": f"Column '{column_name}' has a normality score of {item['value']*100}%.",
                "type": "Outliers",
                "scope": {"perimeter": "column", "value": column_name},
                "level": determine_recommendation_level(
                    1 - item["value"]
                ),  # Convert percentage to proportion
            }
            pack.recommendations.data.append(recommendation)

# Multivariate Outlier Recommendation
# Assuming 'normality_score_dataset' is the key for the dataset's normality score
dataset_normality_score = next(
    (
        item["value"]
        for item in pack.metrics.data
        if item["key"] == "normality_score_dataset"
    ),
    None,
)
if (
    dataset_normality_score is not None
    and dataset_normality_score < normality_threshold
):
    recommendation = {
        "content": f"The dataset '{pack.source_config['name']}' has a normality score of {dataset_normality_score*100}%.",
        "type": "Outliers",
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        "level": determine_recommendation_level(1 - dataset_normality_score),
    }
    pack.recommendations.data.append(recommendation)

pack.recommendations.data.append(
    {
        "content": f"The dataset '{pack.source_config['name']}' has a total of {total_outliers_count} outliers. Check them in output file.",
        "type": "Outliers",
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        "level": determine_recommendation_level(
            total_outliers_count / len(pack.df_source)
        ),
    }
)

pack.recommendations.save()
####################### Export

# Step 1: Compile Univariate Outliers
all_univariate_outliers = pd.DataFrame()
for column, outliers in univariate_outliers.items():
    # Select only the rows that are outliers, preserving the index
    outliers_with_id = pack.df_source.loc[outliers.index, id_columns + [column]].copy()
    # Create a new 'value' column to store the outlier values
    outliers_with_id["value"] = outliers_with_id[column]
    # Add column name as 'OutlierAttribute'
    outliers_with_id["OutlierAttribute"] = column
    # Optionally, if you want to keep the original index as a column
    outliers_with_id["index"] = outliers_with_id.index
    # Drop the original value columns as we have captured it in 'value'
    outliers_with_id = outliers_with_id[
        id_columns + ["index", "OutlierAttribute", "value"]
    ]
    # Concatenate to the all_univariate_outliers DataFrame
    all_univariate_outliers = pd.concat(
        [all_univariate_outliers, outliers_with_id], ignore_index=True
    )

# Step 1: Compile Univariate Outliers
all_univariate_outliers_simple = pd.DataFrame()
for column, outliers in univariate_outliers.items():
    outliers_with_id = pack.df_source.loc[outliers.index, id_columns + [column]].copy()
    outliers_with_id["OutlierAttribute"] = column
    outliers_with_id["index"] = outliers_with_id.index
    all_univariate_outliers_simple = pd.concat(
        [all_univariate_outliers_simple, outliers_with_id], ignore_index=True
    )

# Rearrange columns for all_univariate_outliers
# Ensure the 'value' column is correctly placed if needed here
id_and_other_columns = (
    ["index"]
    + id_columns
    + ["OutlierAttribute", "value"]  # Include "value" in the list
)
all_univariate_outliers = all_univariate_outliers[id_and_other_columns]


# Extracting column labels
columnLabels = all_univariate_outliers.columns.tolist()

# Converting the DataFrame into the desired format without row labels
data_formatted = [
    [{"value": row[col]} for col in all_univariate_outliers.columns]
    for index, row in all_univariate_outliers.iterrows()
]

# The formatted data structure, now without rowLabels
format_structure = {
    "columnLabels": columnLabels,
    "data": data_formatted,
}

# Append the precision, recall, and F1 score to the metrics
pack.metrics.data.extend(
    [
        {
            "key": "outliers_table",
            "value": format_structure,
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
    ]
)

pack.metrics.save()

# Step 2: Compile Multivariate Outliers
multivariate_outliers["index"] = (
    multivariate_outliers.index
)  # Capture the original index
multivariate_outliers["OutlierAttribute"] = "Multivariate"
multivariate_outliers.reset_index(drop=True, inplace=True)

# Rearrange columns for multivariate_outliers
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

# Step 3: Combine Data
all_outliers = pd.concat(
    [all_univariate_outliers_simple, multivariate_outliers], ignore_index=True
)

# Ensure that all_outliers has the same column order
all_outliers = all_outliers[id_and_other_columns]

# Step 4: Save to Excel
# Format the excel file path with source name and current date
if pack.source_config["type"] == "file":
    source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
    current_date = datetime.now().strftime("%Y%m%d")
    excel_file_name = (
        f"{current_date}_outlier_detection_report_{pack.source_config['name']}.xlsx"
    )
    excel_file_path = os.path.join(source_file_dir, excel_file_name)

    # Use this path in the ExcelWriter
    with pd.ExcelWriter(excel_file_path, engine="xlsxwriter") as writer:
        all_univariate_outliers.to_excel(
            writer, sheet_name="Univariate Outliers", index=False
        )
        multivariate_outliers.to_excel(
            writer, sheet_name="Multivariate Outliers", index=False
        )
        all_outliers.to_excel(writer, sheet_name="All Outliers", index=False)

    print(f"Outliers report saved to {excel_file_path}")
