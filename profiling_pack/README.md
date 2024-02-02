# Profiling

This pack uses [ydata Profiling](https://github.com/ydataai/ydata-profiling) python package 🐍 to scan and compute general metrics 📈 on the data.

## Input 📥

### Configuration ⚙️

| Name                   | Type  | Required | Default | Description                                              |
| ---------------------- | ----- | -------- | ------- | -------------------------------------------------------- |
| `jobs.source.skiprows` | `int` | no       | `0`     | The number of rows to skip at the beginning of the file. |


### Source type compatibility 🧩

This pack is compatible with **files** 📁 (``csv``, ``xslx``) and **databases** 🖥️ (``MySQL``, ``PostgreSQL``).

## Analysis 🕵️‍♂️

The pack assesses the data and computes the following metrics:

| Name                         | Description                                                                                        | Scope          | Type                                |
| ---------------------------- | -------------------------------------------------------------------------------------------------- | -------------- | ----------------------------------- |
| `n`                          | Number of records                                                                                  | dataset        | `integer`                           |
| `n_var`                      | Number of variables                                                                                | dataset        | `integer`                           |
| `memory_size`                | Memory size of the dataset                                                                         | dataset        | `integer`                           |
| `record_size`                | Size of a record in bytes                                                                          | dataset        | `integer`                           |
| `n_cells_missing`            | Number of cells empty                                                                              | dataset        | `integer`                           |
| `n_vars_with_missing`        | Number of variables containing missing values                                                      | dataset        | `integer`                           |
| `n_vars_all_missing`         | Number of variables containing 100% missing values                                                 | dataset        | `integer`                           |
| `p_cells_missing`            | Percentage of cells empty                                                                          | dataset        | `float`                             |
| `record_size`                | Number of records                                                                                  | dataset        | `integer`                           |
| `types_unsupported`          | Number of variables with unsupported types                                                         | dataset        | `integer`                           |
| `types_numeric`              | Number of variables with numeric types                                                             | dataset        | `integer`                           |
| `types_text`                 | Number of variables with text types                                                                | dataset        | `integer`                           |
| `score`                      | Completeness Score of the dataset, Average of all var `completeness_score`                         | dataset        | `float`                             |
| `n_distinct`                 | Number of distinct values                                                                          | column         | `integer`                           |
| `n_missing`                  | Number of missing values                                                                           | column         | `integer`                           |
| `p_missing`                  | Percentage of missing values                                                                       | column         | `float`                             |
| `p_distinct`                 | Percentage of distinct values                                                                      | column         | `float`                             |
| `is_unique`                  | Is the variable unique                                                                             | column         | `boolean`                           |
| `n_unique`                   | Number of unique values                                                                            | column         | `integer`                           |
| `p_unique`                   | Percentage of unique values                                                                        | column         | `float`                             |
| `type`                       | Type of the variable                                                                               | column         | `string`                            |
| `hashable`                   | Is the variable hashable                                                                           | column         | `boolean`                           |
| `value_counts_without_nan`   | Value counts without NaN                                                                           | column         | `dict` of value:count               |
| `value_counts_index_sorted`  | `value_counts_without_nan` index sorted                                                            | column         | `dict` of value:count               |
| `ordering`                   | Ordering of the variable    1: ASC / 0: DESC                                                       | column         | `boolean`                           |
| `completeness_score`         | Completeness score of the variable                                                                 | column         | `float`                             |
| `n_negative`                 | Number of negative values                                                                          | column:Numeric | `integer`                           |
| `p_negative`                 | Percentage of negative values                                                                      | column:Numeric | `float`                             |
| `n_zeros`                    | Number of zero values                                                                              | column:Numeric | `integer`                           |
| `p_zeros`                    | Percentage of zero values                                                                          | column:Numeric | `float`                             |
| `n_infinite`                 | Number of infinite values                                                                          | column:Numeric | `integer`                           |
| `p_infinite`                 | Percentage of infinite values                                                                      | column:Numeric | `float`                             |
| `mean`                       | Mean of the variable                                                                               | column:Numeric | `float`                             |
| `min`                        | Minimum value of the variable                                                                      | column:Numeric | `float`                             |
| `max`                        | Maximum value of the variable                                                                      | column:Numeric | `float`                             |
| `range`                      | Range of the variable                                                                              | column:Numeric | `float`                             |
| `sum`                        | Sum of the variable                                                                                | column:Numeric | `float`                             |
| `5%`                         | 5th percentile of the variable                                                                     | column:Numeric | `float`                             |
| `25%`                        | 25th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `50%`                        | 50th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `75%`                        | 75th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `95%`                        | 95th percentile of the variable                                                                    | column:Numeric | `float`                             |
| `std`                        | [Standard deviation](https://en.wikipedia.org/wiki/Standard_deviation) of the variable             | column:Numeric | `float`                             |
| `variance`                   | [Variance](https://en.wikipedia.org/wiki/Variance) of the variable                                 | column:Numeric | `float`                             |
| `mad`                        | [Mean absolute deviation](https://en.wikipedia.org/wiki/Median_absolute_deviation) of the variable | column:Numeric | `float`                             |
| `skewness`                   | [Skewness](https://en.wikipedia.org/wiki/Skewness) of the variable                                 | column:Numeric | `float`                             |
| `kurtosis`                   | [Kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of the variable                                 | column:Numeric | `float`                             |
| `iqr`                        | [Interquartile range](https://en.wikipedia.org/wiki/Interquartile_range) of the variable           | column:Numeric | `float`                             |
| `cv`                         | [Coefficient of variation](https://en.wikipedia.org/wiki/Coefficient_of_variation) of the variable | column:Numeric | `float`                             |
| `monotonic`                  | Is the variable [monotonic](https://en.wikipedia.org/wiki/Monotonic_function)                      | column:Numeric | `boolean`                           |
| `monotonic_increase`         | Is the variable monotonic increasing                                                               | column:Numeric | `boolean`                           |
| `monotonic_decrease`         | Is the variable monotonic decreasing                                                               | column:Numeric | `boolean`                           |
| `monotonic_increase_strict`  | Is the variable strictly monotonic increasing                                                      | column:Numeric | `boolean`                           |
| `monotonic_decrease_strict`  | Is the variable strictly monotonic decreasing                                                      | column:Numeric | `boolean`                           |
| `histogram`                  | Histogram of the variable                                                                          | column:Numeric | `dict` {counts[], bin_edges[]}      |
| `n_category`                 | Number of categories                                                                               | column:Text    | `integer`                           |
| `word_counts`                | Word counts                                                                                        | column:Text    | `dict` of word:count                |
| `n_distinct`                 | Number of distinct values                                                                          | column:Text    | `integer`                           |
| `p_distinct`                 | Percentage of distinct values                                                                      | column:Text    | `float`                             |
| `first_rows`                 | First rows of the variable                                                                         | column:Text    | `dict` rowId:value                  |
| `category_alias_values`      | Alias values of the variable                                                                       | column:Text    | `dict` of value:ALIAS               |
| `category_alias_char_counts` | Alias character counts of the variable                                                             | column:Text    | `dict` of ALIAS:dict of char_count  |
| `max_length`                 | Maximum length of the variable                                                                     | column:Text    | `integer`                           |
| `min_length`                 | Minimum length of the variable                                                                     | column:Text    | `integer`                           |
| `mean_length`                | Mean length of the variable                                                                        | column:Text    | `float`                             |
| `median_length`              | Median length of the variable                                                                      | column:Text    | `float`                             |
| `histogram_length`           | Histogram of the variable length                                                                   | column:Text    | `dict` {counts[], bin_edges[]}      |
| `n_scripts`                  | Number of scripts                                                                                  | column:Text    | `integer`                           |
| `script_counts`              | Script counts    EX: `{'Latin': 918}`                                                              | column:Text    | `dict` of script:count              |
| `script_char_counts`         | Script character counts    EX: `{'Latin': {'a': 918}}`                                             | column:Text    | `dict` of script:dict of char_count |

## Output 📤

### Report 📊

![Profiling Report](https://docs.profiling.ydata.ai/latest/_static/img/ydata-profiling.gif)


The pack generates a report with the computed metrics and the following sections:

- **Overview** 👁️ Get a high-level summary of the dataset, including key metrics and insights.
- **Variables** 📊 Dive into each variable, reviewing distributions, and statistics.
- **Warnings** ⚠️ Highlight potential issues in the data, such as outliers or missing values.
- **Correlations** 🔗 Explore the relationships between different variables.
- **Histogram** 📈 Visualize the distribution of data for each variable.
- **Interactions** 💡 Discover interaction effects between variables.
- **Missing Values** ❓ Identify and address gaps in the data.
- **Sample** 🧪 Take a closer look at a subset of the data.
- **File** 📁 Access and manage the underlying data file.

Filename is `profiling_report_{source_config["name"]}_{current_date}.xlsx`

# Contribute 💡

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs) 👥🚀
