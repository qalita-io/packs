# Data Compare

Data Compare compares the source with another reference source.
The Data Comparison Pack is a robust solution designed to compare and analyze datasets in the healthcare domain. It provides detailed insights into how well the target dataset is represented within the source dataset, offering a thorough comparison based on user-specified columns.

It uses [DataComPy](https://github.com/capitalone/datacompy) library to compare the data.

## Features

- **Configuration-Driven Approach**: Easy to set up through `source_conf.json`, `target_conf.json`, and `pack_conf.json` files, allowing for flexible and dynamic comparison criteria.

- **Data Loading**: Integrated data loading mechanism using `opener.py`, ensuring secure and reliable ingestion of source and target datasets.

- **Comprehensive Data Comparison**: Utilizes `datacompy` to perform an exhaustive comparison between source and target datasets, ensuring high accuracy in data analysis.

- **Insightful Reporting**: Generates a detailed report highlighting differences and similarities between datasets, including DataFrame summaries, Column summaries, Row summaries, and Column comparisons.

- **Metrics Generation**: Parses the generated report to extract key metrics, providing quantitative insights into the datasets' comparison.

- **Score Calculation**: Computes a matching score based on the rate of target rows that match with the source, offering a clear, percentage-based metric to gauge data consistency.

- **Resilient Error Handling**: Implements robust error handling, providing clear feedback and ensuring stability even in case of data discrepancies or configuration issues.

## Output Files
The pack generates the following files as output, offering a comprehensive overview of the comparison:

- `metrics.json`: Contains all the metrics extracted from the comparison, including the matching score and other key statistics.

- `comparison_report.txt`: A human-readable report detailing the differences and similarities between the datasets.

## Usage
This pack is designed to be user-friendly and can be easily integrated into your data analysis pipeline. Ensure the configuration files are set up correctly, and then execute the pack to perform the comparison and generate the metrics.

# Contribute

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs)
