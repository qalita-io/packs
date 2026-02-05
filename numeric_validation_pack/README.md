# Numeric Validation Pack

Validates numeric data against configurable ranges and constraints.

##  Checks Covered

- `number_below_min_value` / `number_above_max_value`
- `number_in_range_percent` / `integer_in_range_percent`
- `negative_values` / `negative_values_percent`
- `min_in_range` / `max_in_range` / `sum_in_range` / `mean_in_range`
- `valid_latitude_percent` / `valid_longitude_percent`
- `invalid_latitude` / `invalid_longitude`

## Configuration

```json
{
  "job": {
    "rules": [
      {"column": "age", "min_value": 0, "max_value": 150},
      {"column": "price", "min_value": 0},
      {"column": "latitude", "type": "latitude"},
      {"column": "longitude", "type": "longitude"},
      {"column": "percentage", "type": "percentage"},
      {"column": "quantity", "type": "non_negative"}
    ],
    "check_negative_values": true
  }
}
```

## Rule Types

| Type | Min Value | Max Value | Description |
|------|-----------|-----------|-------------|
| `latitude` | -90 | 90 | Geographic latitude |
| `longitude` | -180 | 180 | Geographic longitude |
| `percentage` | 0 | 100 | Percentage values |
| `non_negative` | 0 | - | Non-negative numbers |
| (custom) | configurable | configurable | Custom range |

## Metrics Output

- `number_below_min_value`: Count of values below minimum
- `number_above_max_value`: Count of values above maximum
- `number_in_range_percent`: Percentage of values in valid range
- `min_value`: Actual minimum value in column
- `max_value`: Actual maximum value in column
- `sum_value`: Sum of all values
- `mean_value`: Mean of all values
- `negative_values`: Count of negative values
- `negative_values_percent`: Percentage of negative values
- `score`: Overall validity score (0-1)

## License

Proprietary - QALITA SAS
