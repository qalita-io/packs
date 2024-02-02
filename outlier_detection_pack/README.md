# Outlier Detection

## Overview

The Outlier Detection pack focuses on identifying and quantifying outliers within datasets, providing insights into the normality of data distributions. It utilizes the K-Nearest Neighbors (KNN) algorithm from the [PyOD library](https://pyod.readthedocs.io/) for outlier detection in both univariate (column-wise) and multivariate (dataset-wise) contexts.

## Key Features

1. **Univariate Outlier Detection**: Examines each numeric column independently to detect outliers. It calculates a normality score indicating the proportion of inliers (data points that are not outliers) in each column.

2. **Multivariate Outlier Detection**: Considers the entire dataset to detect outliers, providing a holistic view of data normality. It calculates a normality score for the entire dataset, giving a sense of overall data consistency.

3. **Normality Scoring**: Offers a score between 0 and 100% for each column and for the entire dataset. A score of 100% indicates no detected outliers, signifying highly normal data.

4. **Actionable Recommendations**: Generates recommendations when a significant number of outliers are detected in a column or across the entire dataset. These recommendations are stratified into 'high', 'warning', and 'info' levels based on the severity of the detected outliers.

## Workflow Steps

1. **Load Configuration**: Reads configurations from `source_conf.json` and `pack_conf.json` files.

2. **Data Loading**: Uses a custom data loader (defined in `opener.py`) to ingest the dataset into a Pandas DataFrame.

3. **Data Preprocessing**:
   - Identifies and processes non-numeric columns.
   - Applies one-hot encoding to non-numeric columns, preparing data for the KNN model.

4. **Outlier Detection Process**:
   - Performs univariate outlier detection on each numeric column.
   - Conducts multivariate outlier detection on the entire dataset.
   - Calculates normality scores based on the KNN model's output.

5. **Metrics and Recommendations Generation**:
   - Produces `metrics.json` containing normality scores for columns and the dataset.
   - Generates `recommendations.json` with suggestions based on the detected outliers.

6. **Output Artifacts**:
   - `metrics.json`: Includes normality scores for individual columns and the entire dataset.
   - `recommendations.json`: Contains actionable recommendations based on outlier analysis.

## Configuration Notes

Ensure proper setup of `source_conf.json` and `pack_conf.json`, with special attention to the `outlier_threshold` parameter in `pack_conf.json` to tailor the sensitivity of outlier detection.

# Contribute

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs)

