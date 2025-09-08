## FHIR Compliance Pack

Valide la conformité d'un dataset vis-à-vis d'un sous-ensemble HL7 FHIR (par défaut `Patient`).

### Principes
- Mapping configurable des colonnes vers les champs FHIR (`pack_conf.json`)
- Vérifications: champs requis, énumérations, patterns, date ISO, booléens
- Métriques: `score` (validity ratio), `completeness`, `validity_ratio`

### Configuration minimale (`pack_conf.json`)
```
{
  "job": {
    "resource_type": "Patient",
    "field_mappings": {"id": "id", "gender": "gender", "birthDate": "birthDate", "active": "active"},
    "required_fields": ["id"],
    "enums": {"gender": ["male", "female", "other", "unknown"]},
    "patterns": {"id": "^[A-Za-z0-9\-\.]{1,64}$"},
    "date_fields": ["birthDate"],
    "boolean_fields": ["active"]
  }
}
```


