## Great Expectations Pack

Exécute une suite d'expectations Great Expectations sur le dataset chargé via Qalita.

### Config
- `job.suite_name`: nom logique de la suite
- `job.expectations`: liste d'expectations (type + kwargs), ex:
```
{
  "expectation_type": "expect_table_row_count_to_be_between",
  "kwargs": {"min_value": 1}
}
```

### Métriques
- `expectation_result` par expectation
- `score` = ratio de succès


