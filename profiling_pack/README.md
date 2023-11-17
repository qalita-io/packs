# Profiling

This pack uses heavily [ydata Profiling](https://github.com/ydataai/ydata-profiling) python package to scan and compute general metrics on the data.

## Source type compabilitily

### File

| | File.csv | File.xslx |
|---|---|---|
| Status | ok | ok |

### Relationnal

| | Database.MySQL | Database.PostgreSQL | Database.Oracle |
|---|---|---|---|
| Status | planned | planned |  |

### No-SQL

| | Database.Elasticsearch | Database.MongoDB | Database.Cassandra | Database.Redis |
|---|---|---|---|---|
| Status |  |  |  |   |

### Graph

| | Graph.Neo4j |
|---|---|
| Status |  |

## Input configuration

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `metrics_chart_mapping` | `array` | `false` | - | If you want to map a metric to a chart type in the source view. |
| `metrics_chart_mapping[].metric_key` | `string` | `true` | - | The metric name. |
| `metrics_chart_mapping[].chart_type` | `string` | `true` | - | The chart type. Currentlty supported chart types : area_chart, line_chart, text_header |
| `schemas_chart_mapping` | `array` | `false` | - | If you want to map a metric to a minified chart type in the schema info view (at the top of your source). |
| `schemas_chart_mapping[].metric_key` | `string` | `true` | - | The metric name. |
| `schemas_chart_mapping[].chart_type` | `string` | `true` | - | The chart type. Currentlty supported chart types : area_chart, line_chart, text_header |

## Output description

| Name | Type | Description |
|------|------|-------------|
| `metrics` | `array` | The metrics computed by the profiling. |
| `recommendations` | `array` | The metrics computed by the profiling. |
| `schemas` | `array` | The metrics computed by the profiling. |

# Contribute

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs)
