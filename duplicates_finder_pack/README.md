# Duplicates Finder

![Duplicates Finder](https://raw.githubusercontent.com/qalita-io/packs/dev/duplicates_finder_pack/duplicates_finder_banner.png)

Duplicates finder searches for duplicates data and computes metrics.

## Input 📥

### Configuration ⚙️

| Name                              | Type   | Required | Default | Description                                              |
| --------------------------------- | ------ | -------- | ------- | -------------------------------------------------------- |
| `jobs.source.skiprows`            | `int`  | no       | `0`     | The number of rows to skip at the beginning of the file. |
| `jobs.compute_uniqueness_columns` | `list` | no       | `[]`    | The list of columns to compute the uniqueness score.     |
| `jobs.id_columns`                 | `list` | no       | `[]`    | The list of columns to use as identifier.                |

### Source type compatibility 🧩

This pack is compatible with **files** 📁 (``csv``, ``xslx``) and **databases** 🖥️ (``MySQL``, ``PostgreSQL``).

## Analysis 🕵️‍♂️

- **Duplication Score Calculation**: Calculates a duplication score based on the number of duplicate rows in a dataset, helping you understand the extent of data redundancy.

| Name         | Description          | Scope   | Type    |
| ------------ | -------------------- | ------- | ------- |
| `score`      | Duplication score    | Dataset | `float` |
| `duplicates` | Number of duplicates | Dataset | `int`   |

## Output 📤

### Report 📊

The report exports the duplicated data by adding the id column, and groupy by duplicates and sorting them.

Filename is `duplicates_report_{source_config["name"]}_{current_date}.xlsx`

# Contribute 💡

[This pack is part of Qalita Open Source Assets (QOSA) and is open to contribution. You can help us improve this pack by forking it and submitting a pull request here.](https://github.com/qalita-io/packs) 👥🚀
