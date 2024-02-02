import json
import os
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import OneHotEncoder
from pyod.models.knn import KNN

# Load the configuration file
print("Load source_conf.json")
with open("source_conf.json", "r", encoding="utf-8") as file:
    source_config = json.load(file)

if source_config['config']['type'] != 'file':
    raise ValueError("This pack only supports file type configuration.")

source_file_path = source_config['config']['path']
source_file_dir = os.path.dirname(source_file_path)

# Load the pack configuration file
print("Load pack_conf.json")
with open("pack_conf.json", "r", encoding="utf-8") as file:
    pack_config = json.load(file)

# Load data using the opener.py logic
from opener import load_data

df = load_data(source_config, pack_config)

# Fill missing values with mean
for column in df.columns:
    if np.issubdtype(df[column].dtype, np.number):
        df[column].fillna(df[column].mean(), inplace=True)

# Identify columns that still contain NaN values after filling
columns_with_nan = [column for column in df.columns if df[column].isnull().any()]

# Remove columns that still contain NaN values
df.drop(columns=columns_with_nan, inplace=True)

# Check if there are any NaN values left in the dataframe (optional, for sanity check)
if df.isnull().values.any():
    raise ValueError("Unexpected NaN values are still present in the dataframe after filling and dropping.")

############################ Metrics
# Data to be written to JSON
data = []
epsilon = 1e-7  # Small number to prevent division by zero
outlier_threshold = pack_config['job'].get("outlier_threshold", 0.5)

# Dictionary to store univariate outliers for each column
univariate_outliers = {}

# Univariate Outlier Detection
for column in df.columns:
    if np.issubdtype(df[column].dtype, np.number):  # Process only numeric columns
        clf = KNN()
        clf.fit(df[[column]])  # Fit model to the column

        # Get the outlier scores
        scores = clf.decision_function(df[[column]])

        # Calculate the inlier score (100 - outlier score)
        inlier_score = (1 - scores / (scores.max() + epsilon)) # Normalize and convert to percentage

        data.append({
            "key": f"normality_score",
            "value": round(inlier_score.mean().item(),2),  # Get the average score for the column
            "scope": {"perimeter": "column", "value": column},
        })

        # Identify outliers based on the inlier score and threshold
        outliers = df[[column]][inlier_score < outlier_threshold]
        univariate_outliers[column] = outliers

        outlier_count = len(outliers)
        data.append({
            "key": f"outliers",
            "value": outlier_count,
            "scope": {"perimeter": "column", "value": column},
        })

# Identify non-numeric columns
non_numeric_columns = df.select_dtypes(exclude=[np.number]).columns

# Apply OneHotEncoder
encoder = OneHotEncoder(drop='if_binary')
encoded_data = encoder.fit_transform(df[non_numeric_columns])

# If you want a dense array, use .toarray()
encoded_df = pd.DataFrame(encoded_data.toarray(), columns=encoder.get_feature_names_out())

# Drop non-numeric columns from original df and concatenate with encoded_df
df = df.drop(non_numeric_columns, axis=1)
df = pd.concat([df, encoded_df.reset_index(drop=True)], axis=1)

# Multivariate Outlier Detection
clf = KNN()
clf.fit(df)  # Fit model to the entire dataset

# Get the outlier scores
scores = clf.decision_function(df)

# Identify multivariate outliers based on the inlier score and threshold
multivariate_outliers = df[inlier_score < outlier_threshold]

data.append({
    "key": "outliers",
    "value": len(multivariate_outliers),
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
})

inlier_score = (1 - scores / (scores.max() + epsilon))  # Normalize and convert to percentage

data.append({
    "key": "normality_score_dataset",
    "value": round(inlier_score.mean().item(),2),
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
})
data.append({
    "key": "score",
    "value": round(inlier_score.mean().item(),2),
    "scope": {"perimeter": "dataset", "value": source_config["name"]},
})

# Writing data to metrics.json
with open("metrics.json", "w") as file:
    json.dump(data, file, indent=4)

print("metrics.json file created successfully.")

# Define a threshold for considering a data point as an outlier
normality_threshold = pack_config['job']["normality_threshold"]  # Ensure this exists in your pack_conf.json

# Define a function to determine recommendation level based on the proportion of outliers
def determine_recommendation_level(proportion_outliers):
    if proportion_outliers > 0.5:  # More than 20% of data are outliers
        return "high"
    elif proportion_outliers > 0.3:  # More than 10% of data are outliers
        return "warning"
    else:
        return "info"

# Generate recommendations if the number of outliers is significant
recommendations = []

# Univariate Outlier Recommendations
for item in data:
    if item['key'].startswith('normality_score') and 'column' in item['scope']['perimeter']:
        # Check if the normality score is below the threshold
        if item['value'] < normality_threshold:
            column_name = item['scope']['value']
            recommendation = {
                "content": f"Column '{column_name}' has a normality score of {item['value']*100}%. Consider reviewing for outliers.",
                "type": "Outliers",
                "scope": {"perimeter": "column", "value": column_name},
                "level": determine_recommendation_level(1 - item['value']),  # Convert percentage to proportion
            }
            recommendations.append(recommendation)

# Multivariate Outlier Recommendation
# Assuming 'normality_score_dataset' is the key for the dataset's normality score
dataset_normality_score = next((item['value'] for item in data if item['key'] == 'normality_score_dataset'), None)
if dataset_normality_score is not None and dataset_normality_score < normality_threshold:
    recommendation = {
        "content": f"The dataset '{source_config['name']}' has a normality score of {dataset_normality_score*100}%. Consider reviewing for outliers.",
        "type": "Outliers",
        "scope": {"perimeter": "dataset", "value": source_config["name"]},
        "level": determine_recommendation_level(1 - dataset_normality_score),  # Convert percentage to proportion
    }
    recommendations.append(recommendation)

############################ Writing Metrics and Recommendations to Files
if recommendations:
    with open("recommendations.json", "w", encoding="utf-8") as f:
        json.dump(recommendations, f, indent=4)
    print("recommendations.json file created successfully.")

####################### Export
    
# Format the current date as YYYYMMDD
current_date = datetime.now().strftime("%Y%m%d")

# Get the source name from the source_config
source_name = source_config.get("name", "default_source")

# Step 1: Compile Univariate Outliers
id_columns = pack_config['job'].get("id_columns", [])
all_univariate_outliers = pd.DataFrame()

for column, outliers in univariate_outliers.items():
    outliers = outliers.reset_index()  # Reset index to ensure id_columns can be used
    outliers['OutlierAttribute'] = column  # Add a column to specify which attribute is the outlier
    all_univariate_outliers = pd.concat([all_univariate_outliers, outliers], ignore_index=True)

# Step 2: Compile Multivariate Outliers
multivariate_outliers = multivariate_outliers.reset_index()  # Reset index to ensure id_columns can be used
multivariate_outliers['OutlierAttribute'] = 'Multivariate'  # Specify that these are multivariate outliers

# Step 3: Combine Data
all_outliers = pd.concat([all_univariate_outliers, multivariate_outliers], ignore_index=True)

# Ensure id_columns are at the front
id_columns = [col for col in id_columns if col in all_outliers.columns]
other_columns = [col for col in all_outliers.columns if col not in id_columns + ['OutlierAttribute']]
all_outliers = all_outliers[id_columns + ['OutlierAttribute'] + other_columns]

# Format the excel file path with source name and current date
excel_file_name = f'outliers_report_{source_name}_{current_date}.xlsx'
excel_file_path = os.path.join(source_file_dir, excel_file_name)

# Use this path in the ExcelWriter
with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
    all_univariate_outliers.to_excel(writer, sheet_name='Univariate Outliers', index=False)
    multivariate_outliers.to_excel(writer, sheet_name='Multivariate Outliers', index=False)
    all_outliers.to_excel(writer, sheet_name='All Outliers', index=False)

print(f'Outliers report saved to {excel_file_path}')
