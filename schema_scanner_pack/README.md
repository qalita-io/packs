# Schema Scanner Pack

The Schema Scanner Pack is a comprehensive solution designed for profiling and analyzing datasets, particularly focusing on understanding the schema and key metrics of the data. The pack aims to provide clear insights into the structure and quality of data, which is crucial in the healthcare data engineering field where data integrity and accuracy are paramount.

## Overview

The Schema Scanner Pack performs the following main tasks:

1. **Data Loading:** It starts by loading the data using the configuration provided in `source_conf.json` and `pack_conf.json`, ensuring that the data is read correctly and is ready for analysis.

2. **Profiling:** Utilizing the `ProfileReport` from the `ydata_profiling` library, the pack generates a minimal profiling report, providing a quick overview of the data, including types of data, missing values, and other essential metrics.

3. **Metrics Extraction & Normalization:** The pack extracts key metrics from the profiling report. It includes general dataset metrics such as the number of observations and column-specific metrics like the number of non-missing entries. The data is then normalized and rounded where applicable to ensure clarity and precision in the reports.

4. **Schema Understanding:** The pack pays special attention to understanding the schema of the dataset. It identifies the dataset name and the names of all variables (columns) in the dataset, preparing a structured overview of the schema.

5. **Output Generation:** Finally, the pack generates JSON files - `metrics.json` and `schemas.json`. These files contain the normalized metrics and schema information of the dataset, providing a clear, structured, and easy-to-understand representation of the data’s structure and key metrics.

## Key Features

- **Automated Profiling:** Quick and automated generation of data profiles to understand the key characteristics of the dataset at a glance.
- **Custom Metrics & Schema Reporting:** Ability to extract custom metrics and understand the data schema, tailored to the specific needs of healthcare data engineering.
- **Output Standardization:** Provides standardized JSON outputs that can be easily integrated into further data quality frameworks or reporting tools.
- **Scalability:** Designed to handle datasets of various sizes and complexities, ensuring consistent performance and accurate results.


## Source type compabilitily

| type                  | compatibility |
| --------------------- | ------------- |
| File : csv            | ok            |
| File : xslx           | ok            |
| Database : MySQL      | planned       |
| Database : PostgreSQL | planned       |


## Input configuration

| Name                   | Type    | Required | Default | Description                                              |
| ---------------------- | ------- | -------- | ------- | -------------------------------------------------------- |
| `charts.overview`      | `array` | no       | `[]`    | The charts to display in the overview section.           |
| `charts.scoped`        | `array` | no       | `[]`    | The charts to display in the scoped section.             |

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
