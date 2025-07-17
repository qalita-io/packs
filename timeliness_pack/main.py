import json
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
import re
from qalita_core.pack import Pack

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
pack = Pack()
if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("Pour une source de type 'database', il faut spécifier 'table_or_query' dans la config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

############################ Infer Data Types and Count Date Columns

date_columns_count = 0  # Initialize the counter for date columns

def is_date(string):
    # Define patterns for date formats
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",  # yyyy-mm-dd
        r"^\d{4}/\d{2}/\d{2}$",  # yyyy/mm/dd
        r"^\d{2}-\d{2}-\d{4}$",  # mm-dd-yyyy
        r"^\d{2}/\d{2}/\d{4}$",  # dd/mm/yyyy
        r"^\d{2}-\d{2}-\d{4}$",  # dd-mm-yyyy
        r"^\d{2}/\d{2}/\d{4}$",  # mm/dd/yyyy
        r"^\d{4}\.\d{2}\.\d{2}$",  # yyyy.mm.dd
        r"^\d{2}\.\d{2}\.\d{4}$",  # dd.mm.yyyy
        r"^\d{2}\.\d{2}\.\d{4}$",  # mm.dd.yyyy
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",  # yyyy-mm-dd HH:MM:SS
    ]

    # Convert the input to a string before checking
    string = str(string)

    # Check if the string is a year
    if re.match(r"^\d{4}$", string):
        # Check if it's a valid year (e.g., between 1900 and the current year)
        year = int(string)
        if 1900 <= year <= datetime.now().year:
            return "year_only"
        else:
            return False

    # Check if the string matches any of the date patterns
    if any(re.match(pattern, string) for pattern in date_patterns):
        try:
            # If the format is correct, try parsing it
            parse(string)
            return True
        except ValueError:
            return False
    return False


def calculate_timeliness_score(days_since):
    # Score is 1.0 if days_since is 0,
    # and decreases linearly to 0.0 if days_since is 365 or more
    return max(0.0, 1 - (days_since / 365))


for column in pack.df_source.columns:
    unique_values = pack.df_source[column].dropna().unique()
    sample_size = min(10, len(unique_values))
    sample_values = unique_values[:sample_size]

    date_type = set(is_date(value) for value in sample_values)

    if "year_only" in date_type:
        # Handle year-only columns
        year_data = pack.df_source[column].astype(int)
        earliest_year = year_data.min()
        latest_year = year_data.max()

        # Calculate timedelta for the earliest and latest year
        current_year = datetime.now().year
        timedelta_latest_year = (current_year - latest_year) * 365
        timedelta_earliest_year = (current_year - earliest_year) * 365

        # Calculate timeliness_score for the latest year
        timeliness_score_year = calculate_timeliness_score(timedelta_latest_year)

        # Append year-only metrics to metrics_data
        pack.metrics.data.extend(
            [
                {
                    "key": "earliest_year",
                    "value": str(earliest_year),
                    "scope": {"perimeter": "column", "value": column},
                },
                {
                    "key": "latest_year",
                    "value": str(latest_year),
                    "scope": {"perimeter": "column", "value": column},
                },
                {
                    "key": "days_since_earliest_year",
                    "value": str(timedelta_earliest_year),
                    "scope": {"perimeter": "column", "value": column},
                },
                {
                    "key": "days_since_latest_year",
                    "value": str(timedelta_latest_year),
                    "scope": {"perimeter": "column", "value": column},
                },
                {
                    "key": "timeliness_score",
                    "value": str(round(timeliness_score_year, 2)),
                    "scope": {"perimeter": "column", "value": column},
                },
            ]
        )

    elif True in date_type:
        pack.df_source[column] = pd.to_datetime(pack.df_source[column])
        date_columns_count += (
            1  # Increment the counter when a date column is identified
        )

        # Get the earliest and latest date in the column
        earliest_date = pack.df_source[column].min()
        latest_date = pack.df_source[column].max()

        # Skip column if it contains only null values or NaT
        if pd.isnull(earliest_date) or pd.isnull(latest_date):
            continue

        timedelta_latest = (datetime.now() - latest_date).days
        timedelta_earliest = (datetime.now() - earliest_date).days

        # Prepare the information to be appended to the metrics_data list
        date_info_keys = [
            "earliest_date",
            "latest_date",
            "days_since_earliest_date",
            "days_since_latest_date",
        ]
        date_info_values = [
            (
                earliest_date.strftime("%Y-%m-%d")
                if earliest_date and earliest_date is not pd.NaT
                else None
            ),
            (
                latest_date.strftime("%Y-%m-%d")
                if latest_date and latest_date is not pd.NaT
                else None
            ),
            (
                timedelta_earliest
                if earliest_date and earliest_date is not pd.NaT
                else None
            ),
            timedelta_latest if latest_date and latest_date is not pd.NaT else None,
        ]

        for key, value in zip(date_info_keys, date_info_values):
            pack.metrics.data.append(
                {
                    "key": key,
                    "value": str(value) if value is not None else "N/A",
                    "scope": {"perimeter": "column", "value": column},
                }
            )

        # Check for latest date older than one year
        if timedelta_latest > 365:
            pack.recommendations.data.append(
                {
                    "content": f"The latest date in column '{column}' is more than one year old.",
                    "type": "Latest Date far in the past",
                    "scope": {"perimeter": "column", "value": column},
                    "level": "high",
                }
            )

############################ Compute Score Based on Average days_since_latest_date

if date_columns_count > 0:
    if "job" in pack.pack_config and "compute_score_columns" in pack.pack_config["job"]:
        # Get the list of columns to be used for computing the score
        compute_score_columns = pack.pack_config["job"]["compute_score_columns"]
    else:
        # If the list of columns is not specified, use all date columns
        compute_score_columns = pack.df_source.columns

    # Filter metrics_data to get only the days_since_latest_date for the columns to be used for computing the score
    days_since_latest_dates = [
        int(item["value"])
        for item in pack.metrics.data
        if item["key"] == "days_since_latest_date"
        and item["scope"]["value"] in compute_score_columns
    ]

    if days_since_latest_dates:  # Only proceed if there are relevant columns
        # Calculate average days_since_latest_date
        average_days_since_latest = sum(days_since_latest_dates) / len(
            days_since_latest_dates
        )

        # Compute score based on average_days_since_latest
        # Score is 1.0 if average_days_since_latest is 0,
        # and decreases linearly to 0.0 if average_days_since_latest is 365 or more
        score = max(0.0, 1 - (average_days_since_latest / 365))

        # Add 'score' metric to metrics_data
        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
            }
        )
    else:
        print(
            f"No relevant date columns found in the specified scope: {compute_score_columns}. Metric scores will not be computed."
        )
else:
    print("No date columns found. Metric scores will not be computed.")

# Data to be written to JSON
pack.metrics.data.extend(
    [
        {
            "key": "date_columns_count",
            "value": str(date_columns_count),
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
        },
    ]
)

############################ Compute timeliness_score for Each Column

if date_columns_count > 0:
    # Iterate through metrics_data and calculate timeliness_score for each column
    for item in pack.metrics.data:
        if item["key"] == "days_since_latest_date":
            column_name = item["scope"]["value"]
            days_since_latest = int(item["value"])
            timeliness_score = calculate_timeliness_score(days_since_latest)

            # Append timeliness_score to metrics_data
            pack.metrics.data.append(
                {
                    "key": "timeliness_score",
                    "value": str(round(timeliness_score, 2)),
                    "scope": {"perimeter": "column", "value": column_name},
                }
            )

pack.metrics.save()
pack.recommendations.save()