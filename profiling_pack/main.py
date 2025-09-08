from qalita_core.pack import Pack
from qalita_core.utils import (
    denormalize,
    round_if_numeric,
    extract_variable_name,
    determine_level,
)
from qalita_core import sanitize_dataframe_for_parquet
import json
import pandas as pd
import os
from ydata_profiling import ProfileReport
from datetime import datetime
from io import StringIO

# --- Chargement des données ---
# Pour un fichier : pack.load_data("source")
# Pour une base : pack.load_data("source", table_or_query="ma_table")
pack = Pack()
if pack.source_config.get("type") == "database":
    # On récupère la table ou la requête depuis la config ou on la demande explicitement
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

########################### Profiling and Aggregating Results
# Determine the appropriate name for 'dataset' in 'scope'
dataset_scope_name = pack.source_config["name"]

# Normaliser la source en une liste de tuples (nom_dataset, dataframe)
raw_df_source = pack.df_source
configured_table_or_query = pack.source_config.get("config", {}).get("table_or_query")
data_items = []

# Si l'opener renvoie des chemins parquet, les charger avec pandas
def _load_parquet_if_path(obj):
    try:
        if isinstance(obj, str) and obj.lower().endswith((".parquet", ".pq")):
            return pd.read_parquet(obj, engine="pyarrow")
    except Exception:
        pass
    return obj

if isinstance(raw_df_source, list):
    # Mapper chaque entrée potentielle chemin -> DataFrame
    loaded_items = [_load_parquet_if_path(item) for item in raw_df_source]
    # Si la config fournit une liste de noms, l'utiliser si elle correspond en taille
    names = None
    if isinstance(configured_table_or_query, (list, tuple)):
        names = list(configured_table_or_query)
        if len(names) != len(loaded_items):
            names = None
    if names is None:
        names = [f"{dataset_scope_name}_{i+1}" for i in range(len(loaded_items))]
    data_items = list(zip(names, loaded_items))
else:
    data_items = [(dataset_scope_name, _load_parquet_if_path(raw_df_source))]

print(f"Generating profile for {len(data_items)} dataset(s)")

# Conteneurs globaux si besoin d'agréger
all_alerts_records = []

for dataset_name, df in data_items:
    # Sanitize the DataFrame before profiling/serialization to avoid downstream
    # encoding/type issues with libraries expecting homogeneous column types.
    try:
        df = sanitize_dataframe_for_parquet(df)
    except Exception:
        pass
    # Aperçu
    try:
        print(f"Preview for {dataset_name}:")
        print(df.head())
    except Exception:
        pass

    # Profiling pour ce dataset
    profile = ProfileReport(
        df,
        title=f"Profiling Report for {dataset_name}",
        correlations={"auto": {"calculate": False}},
    )

    # Sauvegarde HTML
    html_file_name = f"{dataset_name}_report.html"
    profile.to_file(html_file_name)

    # Pour les sources fichier, on dépose aussi le rapport à côté du fichier source
    if pack.source_config["type"] == "file" and len(data_items) == 1:
        source_file_dir = os.path.dirname(pack.source_config["config"]["path"])
        current_date = datetime.now().strftime("%Y%m%d")
        report_file_path = os.path.join(
            source_file_dir,
            f'{current_date}_profiling_report_{pack.source_config["name"]}.html',
        )
        profile.to_file(report_file_path)
        print(f"Profiling report saved to {report_file_path}")

    # Sauvegarde JSON (structure exportée par ydata_profiling)
    json_file_name = f"{dataset_name}_report.json"
    profile.to_file(json_file_name)

    # Extraire tableaux HTML pour les alertes (si présents)
    try:
        with open(html_file_name, "r", encoding="utf-8") as f:
            html_content = f.read()
            tables = pd.read_html(StringIO(html_content))
    except ValueError as e:
        print(f"No tables found in the HTML report for {dataset_name}: {e}")
        tables = [pd.DataFrame()]

    ############################ Metrics (par dataset)
    # Scores de complétude par colonne
    for col in df.columns:
        non_null_count = max(df[col].notnull().sum(), 0)
        total_count = max(len(df), 1)
        completeness_score = round(non_null_count / total_count, 2)
        pack.metrics.data.append(
            {
                "key": "completeness_score",
                "value": str(completeness_score),
                "scope": {
                    "perimeter": "column",
                    "value": col,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_name,
                    },
                },
            }
        )

    # Charger le JSON du rapport pour extraire les métriques globales et variables
    print(f"Load {dataset_name}_report.json")
    with open(json_file_name, "r", encoding="utf-8") as file:
        report = json.load(file)

    general_data = denormalize(report["table"])
    for key, value in general_data.items():
        if pack.source_config["type"] == "database":
            entry = {
                "key": key,
                "value": round_if_numeric(value),
                "scope": {
                    "perimeter": "dataset",
                    "value": dataset_name,
                    "parent_scope": {
                        "perimeter": "database",
                        "value": pack.source_config["name"],
                    },
                },
            }
        else:
            entry = {
                "key": key,
                "value": round_if_numeric(value),
                "scope": {"perimeter": "dataset", "value": dataset_name},
            }
        pack.metrics.data.append(entry)

    variables_data = report["variables"]
    for variable_name, attributes in variables_data.items():
        for attr_name, attr_value in attributes.items():
            entry = {
                "key": attr_name,
                "value": round_if_numeric(attr_value),
                "scope": {
                    "perimeter": "column",
                    "value": variable_name,
                    "parent_scope": {
                        "perimeter": "dataset",
                        "value": dataset_name,
                    },
                },
            }
            pack.metrics.data.append(entry)

    # Score basé sur p_cells_missing (directement depuis general_data)
    try:
        p_cells_missing = float(general_data.get("p_cells_missing", 0) or 0)
    except Exception:
        p_cells_missing = 0.0
    score_value = max(min(1 - p_cells_missing, 1), 0)

    if pack.source_config["type"] == "database":
        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score_value, 2)),
                "scope": {
                    "perimeter": "dataset",
                    "value": dataset_name,
                    "parent_scope": {
                        "perimeter": "database",
                        "value": pack.source_config["name"],
                    },
                },
            }
        )
    else:
        pack.metrics.data.append(
            {
                "key": "score",
                "value": str(round(score_value, 2)),
                "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
            }
        )

    ############################ Recommendations (par dataset)
    if len(tables) > 2:
        alerts_data = tables[2]
        alerts_data.columns = ["content", "type"]
        alerts_data["scope"] = alerts_data["content"].apply(
            lambda x: {
                "perimeter": "column",
                "value": extract_variable_name(x),
                "parent_scope": {"perimeter": "dataset", "value": dataset_name},
            }
        )
        alerts_data["level"] = alerts_data["content"].apply(determine_level)
    else:
        print(f"No alerts table found in the HTML report for {dataset_name}.")
        alerts_data = pd.DataFrame()

    all_alerts_records.extend(alerts_data.to_dict(orient="records"))

    ############################ Schemas (par dataset)
    for variable_name in report["variables"].keys():
        entry = {
            "key": "column",
            "value": variable_name,
            "scope": {
                "perimeter": "column",
                "value": variable_name,
                "parent_scope": {"perimeter": "dataset", "value": dataset_name},
            },
        }
        pack.schemas.data.append(entry)

    if pack.source_config["type"] == "database":
        pack.schemas.data.append(
            {
                "key": "dataset",
                "value": dataset_name,
                "scope": {
                    "perimeter": "dataset",
                    "value": dataset_name,
                    "parent_scope": {
                        "perimeter": "database",
                        "value": pack.source_config["name"],
                    },
                },
            }
        )
    else:
        pack.schemas.data.append(
            {
                "key": "dataset",
                "value": dataset_name,
                "scope": {"perimeter": "dataset", "value": dataset_name},
            }
        )

############################ Consolidate recommendations across datasets
pack.recommendations.data = all_alerts_records

############################ Writing Results to Files

# if pack.source_config["type"] == "database":
#     pack.schemas.data.append(
#         {
#             "key": "database",
#             "value": pack.source_config["name"],
#             "scope": {"perimeter": "database", "value": pack.source_config["name"]},
#         }
#     )

#     # Compute the aggregated database level completeness score from the datasets score
#     aggregated_score = 0
#     for metric in pack.metrics.data:
#         if metric["key"] == "score":
#             aggregated_score += float(metric["value"])
#     aggregated_score /= len(df_dict)

#     pack.metrics.data.append(
#         {
#             "key": "score",
#             "value": str(round(aggregated_score, 2)),
#             "scope": {"perimeter": "database", "value": pack.source_config["name"]},
#         }
#     )

################## Remove unwanted metrics or recommendations
    
unwanted_keys = ["histogram"]
pack.metrics.data = [item for item in pack.metrics.data if item.get("key") not in unwanted_keys]

unwanted_keys = ["Unsupported"]
pack.recommendations.data = [item for item in pack.recommendations.data if item.get("type") not in unwanted_keys]

pack.metrics.save()
pack.recommendations.save()
pack.schemas.save()
