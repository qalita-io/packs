# Text Validation Pack

Validates text data for length constraints, word counts, and whitespace issues.

##  Checks Covered

- `text_min_length` / `text_max_length` / `text_mean_length`
- `text_length_below_min_length` / `text_length_above_max_length`
- `text_length_in_range_percent`
- `min_word_count` / `max_word_count`
- `empty_text_found` / `whitespace_text_found`
- `null_placeholder_text_found`
- `text_surrounded_by_whitespace_found`

## Configuration

```json
{
  "job": {
    "rules": [
      {"column": "name", "min_length": 2, "max_length": 100},
      {"column": "description", "max_length": 500}
    ],
    "analyze_all_text_columns": true
  }
}
```

## Null Placeholders Detected

The pack detects common null placeholder strings:
- `null`, `NULL`, `Null`
- `none`, `NONE`, `None`
- `n/a`, `N/A`, `NA`, `na`
- `nan`, `NaN`, `NAN`
- `-`, `--`, `---`
- `.`, `..`
- `undefined`, `UNDEFINED`
- `missing`, `MISSING`
- `unknown`, `UNKNOWN`
- `#N/A`, `#NA`, `#NULL!`
- `(blank)`, `(empty)`
- `<null>`, `<NULL>`

## Metrics Output

- `text_min_length`: Minimum text length in column
- `text_max_length`: Maximum text length in column
- `text_mean_length`: Average text length
- `text_length_below_min_length`: Count below minimum constraint
- `text_length_above_max_length`: Count above maximum constraint
- `text_length_in_range_percent`: Percentage within constraints
- `min_word_count`: Minimum word count
- `max_word_count`: Maximum word count
- `empty_text_found`: Count of empty strings
- `whitespace_text_found`: Count of whitespace-only strings
- `null_placeholder_text_found`: Count of null placeholders
- `text_surrounded_by_whitespace_found`: Count with leading/trailing whitespace
- `total_text_issues`: Total issues found
- `score`: Overall validity score (0-1)

## License

Proprietary - QALITA SAS
