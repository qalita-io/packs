from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

pack = Pack()
pack.load_data("source")

float_columns = pack.df_source.select_dtypes(include=["float", "float64"]).columns

# If there are no float columns, return None
if not float_columns.any():
    print("No float columns found. metrics.json will not be created.")
    raise

total_proportion_score = 0  # Initialize total proportion score used for original mean
valid_columns_count = 0  # Count of columns that have at least one non-NaN value

float_total_proportion_score = 0  # Initialize total proportion score for float_mean
valid_points_count = 0  # Total count of valid data points (non-NaN) across all valid columns

for column in float_columns:
    column_data = pack.df_source[column].dropna()
    
    if column_data.empty:
        continue
    
    valid_data_points = len(column_data)  # Number of non-NaN data points in the current column
    decimals_count = column_data.apply(
        lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0
    )
    max_decimals = decimals_count.max()
    most_common_decimals_series = decimals_count.mode()
    
    if most_common_decimals_series.empty:
        proportion_score = 0
    else:
        most_common_decimals = most_common_decimals_series[0]
        proportion_score = decimals_count[decimals_count == most_common_decimals].count() / valid_data_points
    
    total_proportion_score += proportion_score  # For original mean calculation
    valid_columns_count += 1  # Increment valid columns count
    
    # For float_mean calculation:
    float_total_proportion_score += proportion_score * valid_data_points  
    valid_points_count += valid_data_points  

    if max_decimals > 0:
        pack.metrics.data.append(
            {
                "key": "decimal_precision",
                "value": str(max_decimals),  # Maximum number of decimals
                "scope": {"perimeter": "column", "value": column},
            }
        )

    # Always include proportion_score in pack.metrics.data even if max_decimals is 0
    pack.metrics.data.append(
        {
            "key": "proportion_score",
            "value": str(
                round(proportion_score, 2)
            ),  # Proportion of values with the most common decimals count
            "scope": {"perimeter": "column", "value": column},
        }
    )

# Calculate the mean of proportion scores
mean_proportion_score = (
    total_proportion_score / valid_columns_count if valid_columns_count > 0 else 0
)

# Calculate the float mean proportion score considering all data points accurately
float_mean_proportion_score = float_total_proportion_score / valid_points_count if valid_points_count > 0 else 0

pack.metrics.data.append(
    {
        "key": "float_score", 
        "value": str(round(float_mean_proportion_score, 2)),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)

pack.metrics.data.append(
    {
        "key": "score",
        "value": str(round(mean_proportion_score, 2)),
        "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
    }
)

for column in pack.df_source.columns:
    for item in pack.metrics.data:
        if item["scope"]["value"] == column and item["key"] == "proportion_score":
            proportion_score = float(item["value"])
            if proportion_score < 0.9:
                recommendation = {
                    "content": f"Column '{column}' has {(1-proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
                    "type": "Unevenly Rounded Data",
                    "scope": {"perimeter": "column", "value": column},
                    "level": determine_recommendation_level(1 - proportion_score),
                }
                pack.recommendations.data.append(recommendation)

if pack.metrics.data:
    mean_proportion_score = float(pack.metrics.data[-1]["value"])
    if mean_proportion_score < 0.9:
        recommendation = {
            "content": f"The dataset has {(1-mean_proportion_score)*100:.2f}% of data that are not rounded to the same number of decimals.",
            "type": "Unevenly Rounded Data",
            "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
            "level": determine_recommendation_level(1 - mean_proportion_score),
        }
        pack.recommendations.data.append(recommendation)

pack.metrics.save()
pack.recommendations.save()
