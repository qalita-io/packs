# Timeliness Pack

## Overview
This pack is designed for assessing the quality of data in a dataset, particularly focusing on the timeliness of date columns. It performs a series of checks on a given dataset to identify date columns, evaluate the recency of data in these columns, and calculate scores representing the timeliness of the dataset. The pack also generates recommendations based on the analysis to provide actionable insights.

## Features

- **Date Column Identification**: Utilizes regex patterns and date parsing to accurately identify columns that contain date information, minimizing false positives.

- **Timeliness Analysis**: For each date column, the pack calculates:
    - `earliest_date`: The earliest date in the column.
    - `latest_date`: The latest date in the column.
    - `days_since_earliest_date`: Number of days from the earliest date to the current date.
    - `days_since_latest_date`: Number of days from the latest date to the current date.

- **Score Calculation**:
    - `Overall Score`: Represents the overall timeliness of the dataset. It's computed based on the average `days_since_latest_date` across all date columns. A score of 1.0 indicates very recent data, and it linearly decreases to 0.0 as the average `days_since_latest_date` approaches 365 days or more.
    - `Timeliness Score per Column`: Similar to the overall score but calculated individually for each date column.

- **Recommendations**: Generates high-level recommendations if any date column's latest data is older than one year, prompting a review of data collection processes or data sources.
