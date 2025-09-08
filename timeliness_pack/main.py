import json
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
import re
from qalita_core.pack import Pack
import pandas as pd

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

# Safely convert a Series to datetime, coercing invalid/out-of-bounds values
def safe_to_datetime(series):
    try:
        # Pandas >=2.0 supports format="mixed" for heterogeneous inputs
        return pd.to_datetime(series, errors="coerce", format="mixed")
    except Exception:
        try:
            # Fallback: be lenient and coerce problematic entries
            return pd.to_datetime(series, errors="coerce", dayfirst=True)
        except Exception:
            return pd.to_datetime(series, errors="coerce")

def _load_parquet_if_path(obj):
    try:
        if isinstance(obj, str) and obj.lower().endswith((".parquet", ".pq")):
            return pd.read_parquet(obj, engine="pyarrow")
    except Exception:
        pass
    return obj

if isinstance(raw_df_source, list):
    names = pack.source_config.get("config", {}).get("table_or_query")
    loaded = [_load_parquet_if_path(x) for x in raw_df_source]
    if isinstance(names, (list, tuple)) and len(names) == len(loaded):
        dataset_items = list(zip(list(names), loaded))
    else:
        base = pack.source_config["name"]
        dataset_items = [(f"{base}_{i+1}", df) for i, df in enumerate(loaded)]
else:
    dataset_items = [(pack.source_config["name"], _load_parquet_if_path(raw_df_source))]

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
            converted = safe_to_datetime(df_curr[column])
            # Skip if no valid dates after coercion
            if not converted.notna().any():
                continue
            df_curr[column] = converted
            date_columns_count += 1
            earliest_date = converted.min()
            latest_date = converted.max()
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

############################ (moved) Dataset score will be computed after column timeliness_score

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
            dataset_label = (
                item.get("scope", {}).get("parent_scope", {}).get("value")
                or pack.source_config["name"]
            )
            days_since_latest = int(item["value"])
            timeliness_score = calculate_timeliness_score(days_since_latest)

            # Append timeliness_score to metrics_data
            pack.metrics.data.append(
                {
                    "key": "timeliness_score",
                    "value": str(round(timeliness_score, 2)),
                    "scope": {
                        "perimeter": "column",
                        "value": column_name,
                        "parent_scope": {"perimeter": "dataset", "value": dataset_label},
                    },
                }
            )

############################ Compute Dataset Score Based on Average timeliness_score

if date_columns_count > 0:
    compute_score_columns = pack.pack_config.get("job", {}).get("compute_score_columns")
    # Treat missing or empty list as "use all columns"
    if not compute_score_columns:
        compute_score_columns = None

    # Collect column timeliness_score metrics
    ts_metrics = [
        m
        for m in pack.metrics.data
        if m.get("key") == "timeliness_score"
        and m.get("scope", {}).get("perimeter") == "column"
    ]

    dataset_to_scores = {}
    for m in ts_metrics:
        scope = m.get("scope", {})
        dataset_label = scope.get("parent_scope", {}).get("value") or pack.source_config["name"]
        column_name = scope.get("value")
        if compute_score_columns is None or column_name in compute_score_columns:
            try:
                score_value = float(m.get("value", 0))
            except Exception:
                continue
            dataset_to_scores.setdefault(dataset_label, []).append(score_value)

    for dataset_label, scores in dataset_to_scores.items():
        if not scores:
            continue
        average_score = sum(scores) / len(scores)
        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(average_score, 2)),
                "scope": {"perimeter": "dataset", "value": dataset_label},
            }
        )

pack.metrics.save()
pack.recommendations.save()