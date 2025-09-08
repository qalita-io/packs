## dbt Checks Pack

Exécute `dbt test` et agrège les résultats depuis `target/run_results.json`.

### Config (pack_conf.json)
- `project_dir`: répertoire du projet dbt (par défaut `.`)
- `profiles_dir`: chemin des profiles dbt (par défaut `~/.dbt`)
- `target`: cible dbt (optionnel)
- `models`: sélection de modèles/tests (optionnel)
- `threads`: parallélisme (par défaut 4)
- `vars`: dictionnaire de variables dbt (optionnel)

### Métriques
- `tests_total`, `tests_passed`, `tests_failed`
- `score` = ratio de tests passés


