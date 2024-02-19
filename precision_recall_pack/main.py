"""
Main file for pack
"""
import json
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

########################### Loading Data

# Load the configuration file
print("Load source_conf.json")
with open("source_conf.json", "r", encoding="utf-8") as file:
    source_config = json.load(file)

# Load the pack configuration file
print("Load pack_conf.json")
with open("pack_conf.json", "r", encoding="utf-8") as file:
    pack_config = json.load(file)

# Load data using the opener.py logic
from opener import load_data

df = load_data(source_config, pack_config)

############################ Metrics

############################ Recommendations

######################## Export:
