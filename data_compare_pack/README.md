# Data Compare

Data Compare compares the source with another reference source.
The Data Comparison Pack is a robust solution designed to compare and analyze datasets in the healthcare domain. It provides detailed insights into how well the target dataset is represented within the source dataset, offering a thorough comparison based on user-specified columns.

It uses [DataComPy](https://github.com/capitalone/datacompy) library to compare the data.

## Analysis 🕵️‍♂️

| Name         | Description          | Scope   | Type    |
| ------------ | -------------------- | ------- | ------- |
| `score`      | Duplication score    | Dataset | `float` |
| `precision` | the portion of rows in the target that are correctly represented in the source dataset | Dataset | `int`   |
| `recall` | the portion of rows in the source that are correctly represented in the target dataset | Dataset | `int`   |
| `f1_score` | the harmonic mean of precision and recall | Dataset | `int`   |

## Output Files

The pack generates the following files as output, offering a comprehensive overview of the comparison:

- `metrics.json`: Contains all the metrics extracted from the comparison, including the matching score and other key statistics.
- `comparison_report.xlsx`: A human-readable report detailing the differences and similarities between the datasets.

## Usage

This pack is designed to be user-friendly and can be easily integrated into your data analysis pipeline. Ensure the configuration files are set up correctly, and then execute the pack to perform the comparison and generate the metrics.

# Contribute

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs)
