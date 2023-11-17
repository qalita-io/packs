import yaml

with open('properties.yaml', 'r') as f:
    data = yaml.safe_load(f)
    print(data['name'])
