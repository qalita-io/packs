# Profiling

This pack uses heavily [ydata Profiling](https://github.com/ydataai/ydata-profiling) python package to scan and compute general metrics on the data.

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
| `jobs.source.skiprows` | `int`   | no       | `0`     | The number of rows to skip at the beginning of the file. |
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

# Contribute

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs)
