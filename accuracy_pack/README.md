# Accuracy

## Overview

This pack assesses the precision of float columns within a dataset, providing a granular view of data quality. The script computes the maximum number of decimal places for each float column and generates a normalized score representing the precision level of the data.

## Input 📥

### Configuration ⚙️

| Name                   | Type   | Required | Default | Description                                              |
| ---------------------- | ------ | -------- | ------- | -------------------------------------------------------- |
| `jobs.source.skiprows` | `int`  | no       | `0`     | The number of rows to skip at the beginning of the file. |
| `jobs.id_columns`      | `list` | no       | `[]`    | The list of columns to use as identifier.                |

### Source type compatibility 🧩

This pack is compatible with **files** 📁 (``csv``, ``xslx``).

## Analysis 🕵️‍♂️

- **Precision Calculation**: Computes the maximum number of decimal places for each float value in float columns.
- **Score Normalization**: Normalizes the precision values to a 0-1 scale, providing a standardized precision score for each column.

| Name                | Description                                       | Scope   | Type    |
| ------------------- | ------------------------------------------------- | ------- | ------- |
| `score`             | Accuracy score                                    | Dataset | `float` |
| `decimal_precision` | Number of maximum decimals seen for this variable | Column  | `int`   |
| `proportion_score`  | Proportion of values with maximum decimals        | Column  | `float` |

## Output 📤

### Report 📊

This pack doesn't generate any output or report.

# Contribute 💡

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs) 👥🚀
