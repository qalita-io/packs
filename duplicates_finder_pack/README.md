# Duplicates Finder

Duplicates finder searches for duplicates data and computes metrics.
It only find duplicates for the whole columns,

## Features

- **Duplication Score Calculation**: Calculates a duplication score based on the number of duplicate rows in a dataset, helping you understand the extent of data redundancy.
- **Inversion of Duplication Score**: Transforms the duplication score to represent the uniqueness of the dataset, where a higher score indicates higher uniqueness.
- **Recommendation Generation**: Provides actionable recommendations if the duplication rate exceeds a certain threshold, guiding you towards data cleaning and quality improvement.

## Workflow

1. **Configuration Loading**: The pack starts by loading configuration details from `source_conf.json` and `pack_conf.json`, which contain source and pack-related configurations, respectively.
2. **Data Loading**: Utilizes `opener.py` to load the dataset as per the configurations.
3. **Duplication Analysis**:
    - Calculates the total number of duplicate rows in the dataset.
    - Computes the original duplication score and its inverted counterpart to reflect data uniqueness.
4. **Recommendation Provision**: Generates recommendations if the inverted duplication score is below the threshold (0.9 by default), suggesting a review of the dataset for data cleaning.
5. **Result Storage**: Stores the calculated metrics and recommendations in `metrics.json` and `recommendations.json`, respectively, providing an easy way to access and review the results.

## Source type compabilitily

| type                  | compatibility |
| --------------------- | ------------- |
| File : csv            | ok            |
| File : xslx           | ok            |
| Database : MySQL      | planned       |
| Database : PostgreSQL | planned       |

## Input configuration

| Name              | Type    | Required | Default | Description                                    |
| ----------------- | ------- | -------- | ------- | ---------------------------------------------- |
| `charts.overview` | `array` | no       | `[]`    | The charts to display in the overview section. |
| `charts.scoped`   | `array` | no       | `[]`    | The charts to display in the scoped section.   |

### Attributes configuration for charts

| Name            | Type      | Required | Default | Description                                                                             |
| --------------- | --------- | -------- | ------- | --------------------------------------------------------------------------------------- |
| `metric_key`    | `string`  | yes      | -       | The metric key to display.                                                              |
| `chart_type`    | `string`  | yes      | -       | The chart type to display. See                                                          |
| `display_title` | `boolean` | no       | -       | The title to display on the chart                                                       |
| `justify`       | `boolean` | no       | -       | If you want to the chart to be displayed aligned in justify mode next to others charts. |

## Output description

| Name              | Type    | Description                            |
| ----------------- | ------- | -------------------------------------- |
| `metrics`         | `array` | The metrics computed by the profiling. |
| `recommendations` | `array` | The metrics computed by the profiling. |
| `schemas`         | `array` | The metrics computed by the profiling. |


