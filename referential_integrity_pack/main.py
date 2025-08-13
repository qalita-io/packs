from qalita_core.pack import Pack

pack = Pack()

if pack.source_config.get("type") == "database":
    table_or_query = pack.source_config.get("config", {}).get("table_or_query")
    if not table_or_query:
        raise ValueError("For a 'database' type source, you must specify 'table_or_query' in the config.")
    pack.load_data("source", table_or_query=table_or_query)
else:
    pack.load_data("source")

relations = pack.pack_config.get("job", {}).get("relations", [])
missing_total = 0
checked_total = 0

for rel in relations:
    parent_key = rel["parent"]["key"]
    child_key = rel["child"]["key"]
    if not isinstance(parent_key, list):
        parent_key = [parent_key]
    if not isinstance(child_key, list):
        child_key = [child_key]

    parent_df = pack.df_source if rel["parent"]["source"] == "source" else pack.df_target
    child_df = pack.df_source if rel["child"]["source"] == "source" else pack.df_target

    parent_tuples = set(map(tuple, parent_df[parent_key].dropna().values.tolist()))
    child_tuples = list(map(tuple, child_df[child_key].fillna("__NULL__").values.tolist()))

    missing_fks = [t for t in child_tuples if t not in parent_tuples]
    missing_total += len(missing_fks)
    checked_total += len(child_tuples)

    pack.metrics.data.append({
        "key": "missing_foreign_keys",
        "value": len(missing_fks),
        "scope": {"perimeter": "dataset", "value": rel["child"]["table"]},
    })

score = 1.0 if checked_total == 0 else max(0.0, 1 - (missing_total / checked_total))
pack.metrics.data.append({
    "key": "score",
    "value": str(round(score, 2)),
    "scope": {"perimeter": "dataset", "value": pack.source_config["name"]},
})

pack.metrics.save()


