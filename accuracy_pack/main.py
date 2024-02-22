from qalita_core.pack import Pack
from qalita_core.utils import determine_recommendation_level

pack = Pack()
pack.load_data("source")

float_columns = pack.df_source.select_dtypes(include=["float", "float64"]).columns

# If there are no float columns, return None
if not float_columns.any():
    print("No float columns found. metrics.json will not be created.")
    raise

total_proportion_score = 0  # Initialize total proportion score
valid_columns_count = 0  # Count of columns that have at least one non-NaN value

for column in float_columns:
    column_data = pack.df_source[column].dropna()

    # Skip the column if it only contains NaN values
    if column_data.empty:
        continue

    decimals_count = column_data.apply(
        lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0
    )
    max_decimals = decimals_count.max()
    most_common_decimals_series = decimals_count.mode()

    # Handle the scenario when the mode() returns an empty series
    if most_common_decimals_series.empty:
        print(f"No common decimal count found for column {column}.")
        most_common_decimals = 0
        proportion_score = 0
    else:
        most_common_decimals = most_common_decimals_series[
            0
        ]  # Get the most common decimals count
        proportion_score = decimals_count[
            decimals_count == most_common_decimals
        ].count() / len(decimals_count)

    total_proportion_score += proportion_score  # Add proportion score to the total
    valid_columns_count += 1  # Increment valid columns count

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

# Add the mean proportion score to the precision data
pack.metrics.data.append(
    {
        "key": "score",
        "value": str(round(mean_proportion_score, 2)),  # Mean proportion score
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

# ######################## Export:
# # Step 1: Filter the DataFrame based on precision recommendations

# id_columns = pack_config.get('job', {}).get('id_columns', [])

# # For simplicity, let's assume that columns with a proportion score lower than 0.9 need attention
# columns_to_check = [item["scope"]["value"] for item in pack.metrics.data if item["key"] == "proportion_score" and float(item["value"]) < 0.9]

# # Filter the DataFrame for rows that don't meet the rounding criteria in the specified columns
# expected_precision = float(pack.metrics.data[1]["value"])
# rows_with_rounding_issues = df[df[columns_to_check].applymap(lambda x: isinstance(x, float) and (len(str(x).split(".")[1]) if '.' in str(x) else 0) != expected_precision)]

# # Check if there are rows with rounding issues
# if rows_with_rounding_issues.empty:
#     print("No rounding issues found. No report will be generated.")
# else:
#     # If there are rows with rounding issues, proceed with sorting and exporting
#     rows_with_rounding_issues = rows_with_rounding_issues.sort_values(by=columns_to_check)

#     # Step 3: Set index or create 'index' column for the Excel export
#     if id_columns:
#         # Ensure all id_columns are in the DataFrame columns
#         valid_id_columns = [col for col in id_columns if col in rows_with_rounding_issues.columns]
#         if not valid_id_columns:
#             print("None of the specified 'id_columns' are in the DataFrame. Using default index.")
#             rows_with_rounding_issues = rows_with_rounding_issues.reset_index(drop=True)
#         else:
#             rows_with_rounding_issues = rows_with_rounding_issues.set_index(valid_id_columns)
#     else:
#         # If 'id_columns' is not provided or is empty, create an 'index' column with the original DataFrame's index
#         rows_with_rounding_issues = rows_with_rounding_issues.reset_index()

#     # Continue with the export process
#     if source_config['type'] == 'file':
#         source_file_dir = os.path.dirname(source_config['config']['path'])
#         current_date = datetime.now().strftime("%Y%m%d")
#         report_file_path = os.path.join(source_file_dir, f'rounding_issues_report_{source_config["name"]}_{current_date}.xlsx')

#         # Export rows with rounding issues to an Excel file
#         rows_with_rounding_issues.to_excel(report_file_path, index=False)  # Set index=False as 'original_index' is now a column
#         print(f"Rows with rounding issues have been exported to {report_file_path}")
