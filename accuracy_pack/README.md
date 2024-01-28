# Accuracy

## Overview
This pack assesses the precision of float columns within a dataset, providing a granular view of data quality. The script computes the maximum number of decimal places for each float column and generates a normalized score representing the precision level of the data. The results are saved in `metrics.json`, with each float column's precision score detailed individually.

## Features
- **Precision Calculation**: Computes the maximum number of decimal places for each float value in float columns.
- **Score Normalization**: Normalizes the precision values to a 0-1 scale, providing a standardized precision score for each column.
- **Metrics Generation**: Outputs a `metrics.json` file containing precision scores for each float column, enhancing the interpretability of data quality.

## Setup
Before running the script, ensure that the following files are properly configured:
- `source_conf.json`: Configuration file for the source data.
- `pack_conf.json`: Configuration file for the pack.
- Data file: The data to be analyzed, lo aded using `opener.py`.

## Usage
To use this pack, follow these steps:
1. Ensure all prerequisite files (`source_conf.json`, `pack_conf.json`, and the data file) are in place.
2. Run the script with the appropriate Python interpreter.
3. Review the generated `metrics.json` for precision metrics of the dataset.

## Output
- `metrics.json`: Contains precision scores for each float column in the dataset. The structure of the output is as follows:
  ```json
  [
      {
          "key": "decimal_precision",
          "value": "<precision_score>",
          "scope": {
              "perimeter": "column",
              "value": "<column_name>"
          },
      },
      ...
  ]
