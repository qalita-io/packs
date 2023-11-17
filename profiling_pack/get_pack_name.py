"""
Get the name of the pack from the properties.yaml file
"""

import yaml

with open("properties.yaml", "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)
    print(data["name"])
