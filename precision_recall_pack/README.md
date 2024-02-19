# Precision and Recall

This pack computes the **precision** and **recall** scores for a dataset.

* The ``precision`` and recall scores are used to evaluate the performance of a model. The precision score is the ratio of the number of true positive predictions to the number of true positive predictions plus the number of false positive predictions.
* The ``recall`` score is the ratio of the number of true positive predictions to the number of true positive predictions plus the number of false negative predictions.

## Input 📥

### Configuration ⚙️

| Name                                    | Type   | Required | Default | Description                                                    |
| --------------------------------------- | ------ | -------- | ------- | -------------------------------------------------------------- |
| `jobs.source.skiprows`                  | `int`  | no       | `0`     | The number of rows to skip at the beginning of the file.       |
| `jobs.compute_precision_recall_columns` | `list` | no       | `[]`    | The list of columns to compute the PRECISION and RECALL score. |
| `jobs.id_columns`                       | `list` | no       | `[]`    | The list of columns to use as identifier.                      |

### Source type compatibility 🧩

This pack is compatible with **files** 📁 (``csv``, ``xslx``) and **databases** 🖥️ (``MySQL``, ``PostgreSQL``).

## Analysis 🕵️‍♂️

| Name              | Description          | Scope   | Type    |
| ----------------- | -------------------- | ------- | ------- |
| `score`           | Duplication score    | Dataset | `float` |
| `precision_score` | Precision score      | Dataset | `float` |
| `recall_score`    | Recall score         | Dataset | `float` |

## Output 📤

### Report 📊

Filename is `{current_date}_preicision_recall_report_{source_config["name"]}.xlsx`

# Contribute 💡

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs) 👥🚀
