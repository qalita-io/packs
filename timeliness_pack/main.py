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
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
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


raw_df_source = pack.df_source
configured = pack.pack_config.get("job", {}).get("compute_score_columns")
if isinstance(raw_df_source, list):
    dataset_items = []
    names = pack.source_config.get("config", {}).get("table_or_query")
    if isinstance(names, (list, tuple)) and len(names) == len(raw_df_source):
        dataset_items = list(zip(list(names), raw_df_source))
    else:
        base = pack.source_config["name"]
        dataset_items = [(f"{base}_{i+1}", df) for i, df in enumerate(raw_df_source)]
else:
    dataset_items = [(pack.source_config["name"], raw_df_source)]

for dataset_label, df_curr in dataset_items:
    for column in df_curr.columns:
        unique_values = df_curr[column].dropna().unique()
        sample_size = min(10, len(unique_values))
        sample_values = unique_values[:sample_size]

        date_type = set(is_date(value) for value in sample_values)

        if "year_only" in date_type:
            year_data = df_curr[column].astype(int)
            earliest_year = year_data.min()
            latest_year = year_data.max()
            current_year = datetime.now().year
            timedelta_latest_year = (current_year - latest_year) * 365
            timedelta_earliest_year = (current_year - earliest_year) * 365
            timeliness_score_year = calculate_timeliness_score(timedelta_latest_year)
            pack.metrics.data.extend(
                [
                    {
                        "key": "earliest_year",
                        "value": str(earliest_year),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    },
                    {
                        "key": "latest_year",
                        "value": str(latest_year),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    },
                    {
                        "key": "days_since_earliest_year",
                        "value": str(timedelta_earliest_year),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    },
                    {
                        "key": "days_since_latest_year",
                        "value": str(timedelta_latest_year),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    },
                    {
                        "key": "timeliness_score",
                        "value": str(round(timeliness_score_year, 2)),
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    },
                ]
            )

        elif True in date_type:
            df_curr[column] = pd.to_datetime(df_curr[column])
            date_columns_count += 1
            earliest_date = df_curr[column].min()
            latest_date = df_curr[column].max()
            if pd.isnull(earliest_date) or pd.isnull(latest_date):
                continue
            timedelta_latest = (datetime.now() - latest_date).days
            timedelta_earliest = (datetime.now() - earliest_date).days
            date_info_keys = [
                "earliest_date",
                "latest_date",
                "days_since_earliest_date",
                "days_since_latest_date",
            ]
            date_info_values = [
                (earliest_date.strftime("%Y-%m-%d") if earliest_date and earliest_date is not pd.NaT else None),
                (latest_date.strftime("%Y-%m-%d") if latest_date and latest_date is not pd.NaT else None),
                (timedelta_earliest if earliest_date and earliest_date is not pd.NaT else None),
                (timedelta_latest if latest_date and latest_date is not pd.NaT else None),
            ]
            for key, value in zip(date_info_keys, date_info_values):
                pack.metrics.data.append(
                    {
                        "key": key,
                        "value": str(value) if value is not None else "N/A",
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                    }
                )
            if timedelta_latest > 365:
                pack.recommendations.data.append(
                    {
                        "content": f"The latest date in column '{column}' is more than one year old.",
                        "type": "Latest Date far in the past",
                        "scope": {"perimeter": "column", "value": column, "parent_scope": {"perimeter": "dataset", "value": dataset_label}},
                        "level": "high",
                    }
                )

############################ Compute Score Based on Average days_since_latest_date

if date_columns_count > 0:
    if "job" in pack.pack_config and "compute_score_columns" in pack.pack_config["job"]:
        compute_score_columns = pack.pack_config["job"]["compute_score_columns"]
    else:
        compute_score_columns = None  # use all date columns per dataset

    # Grouper par dataset et calculer un score par dataset
    # Extraire uniquement les métriques days_since_latest_date
    ds_latest = [m for m in pack.metrics.data if m["key"] == "days_since_latest_date"]
    # Regrouper par dataset label
    dataset_to_values = {}
    for m in ds_latest:
        scope = m.get("scope", {})
        dataset_label = scope.get("parent_scope", {}).get("value") or scope.get("value")
        col = scope.get("value")
        if compute_score_columns is None or col in compute_score_columns:
            dataset_to_values.setdefault(dataset_label, []).append(int(m["value"]))

    for dataset_label, values in dataset_to_values.items():
        if not values:
            continue
        average_days_since_latest = sum(values) / len(values)
        score = max(0.0, 1 - (average_days_since_latest / 365))
        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )

# Data to be written to JSON
# Ajoute un compteur global approximatif (toutes tables confondues)
pack.metrics.data.append(
    {
        "key": "date_columns_count",
        "value": str(date_columns_count),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
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