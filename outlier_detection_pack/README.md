# Outlier Detection

The Outlier Detection pack focuses on identifying and quantifying outliers within datasets, providing insights into the normality of data distributions. It utilizes the K-Nearest Neighbors (KNN) algorithm from the [PyOD library](https://pyod.readthedocs.io/) for outlier detection in both univariate (column-wise) and multivariate (dataset-wise) contexts.

<!-- ![Outlier Detection](https://pyod.readthedocs.io/en/latest/_images/ALL.png) -->

## Input 📥

### Configuration ⚙️

| Name                   | Type   | Required | Default | Description                                                                                                                      |
| ---------------------- | ------ | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `jobs.source.skiprows` | `int`  | no       | `0`     | The number of rows to skip at the beginning of the file.                                                                         |
| `jobs.normality_threshold`  | `int`  | no       | `0.9`   | The threshold for the normality score.  If there is a proportion of outliers bellow this threshold, it creates a recommendation. |
| `jobs.id_columns`           | `list` | no       | `[]`    | The list of columns to be used as an identifier.                                                                                 |
| `jobs.outlier_threshold`    | `int`  | no       | `0.5`   | The threshold for detecting outliers based on the inlier score `inlier_score = 1 - scores / (scores.max() + epsilon)`.           |

### Source type compatibility 🧩

This pack is compatible with **files** 📁 (``csv``, ``xslx``).

## Analysis 🕵️‍♂️

The pack assesses the data and computes the following metrics:

* **Univariate Outlier Detection**: Examines each numeric column independently to detect outliers. It calculates a normality score indicating the proportion of inliers (data points that are not outliers) in each column.
* **Multivariate Outlier Detection**: Considers the entire dataset to detect outliers, providing a holistic view of data normality. It calculates a normality score for the entire dataset, giving a sense of overall data consistency.
* **Normality Scoring**: Offers a score between 0 and 100% for each column and for the entire dataset. A score of 100% indicates no detected outliers, signifying highly normal data.
* **Actionable Recommendations**: Generates recommendations when a significant number of outliers are detected in a column or across the entire dataset. These recommendations are stratified into 'high', 'warning', and 'info' levels based on the severity of the detected outliers.

| Name                      | Description                                                 | Scope   | Type    |
| ------------------------- | ----------------------------------------------------------- | ------- | ------- |
| `normality_score_dataset` | The normality score for the entire dataset.                 | Dataset | `float` |
| `score`                   | The aggregated average normality score for each column.     | Dataset | `float` |
| `normality_score`         | The normality score for each column and the entire dataset. | Column  | `float` |
| `outliers`                | The number of outliers detected in each column.             | Column  | `int`   |

## Output 📤

### Report 📊

The pack generates a report containing the following insights:

* **Univariate Outlier Detection**: A summary of the normality score for each numeric column, indicating the proportion of inliers in each column.
* **Multivariate Outlier Detection**: A summary of the normality score for the entire dataset, indicating the proportion of inliers across the entire dataset.

Filename is `outliers_report_{source_config["name"]}_{current_date}.xlsx`

# Contribute 💡

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs) 👥🚀
