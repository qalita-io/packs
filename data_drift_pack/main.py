from qalita_core.pack import Pack
from scipy import stats
import numpy as np

pack = Pack()

if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

if pack.target_config.get("type") == "database":
    table_or_query = pack.target_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type target, you must specify 'table_or_query' in the config.")
    pack.load_data("target", table_or_query=table_or_query)
else:
    pack.load_data("target")

ref_df = pack.df_source
cur_df = pack.df_target

if isinstance(ref_df, list) or isinstance(cur_df, list):
    ref_df = ref_df[0] if isinstance(ref_df, list) else ref_df
    cur_df = cur_df[0] if isinstance(cur_df, list) else cur_df

numeric_columns = [c for c in ref_df.columns if np.issubdtype(ref_df[c].dropna().dtype, np.number) and c in cur_df.columns]

p_values = []
for col in numeric_columns:
    ref = ref_df[col].dropna().values
    cur = cur_df[col].dropna().values
    if len(ref) == 0 or len(cur) == 0:
        continue
    _, p_value = stats.ks_2samp(ref, cur, alternative="two-sided", method="auto")
    p_values.append(p_value)
    pack.metrics.data.append({
        "key": "p_value",
        "value": str(round(float(p_value), 6)),
        "scope": {"perimeter": "column", "value": col, "parent_scope": {"perimeter": "dataset", "value": pack.source_config["name"]}},
    })

if p_values:
    score = float(np.mean([1.0 if p >= 0.05 else 0.0 for p in p_values]))
else:
    score = 1.0

pack.metrics.data.append({
    "key": "score",
    "value": str(round(score, 2)),
    "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
})

pack.metrics.save()


