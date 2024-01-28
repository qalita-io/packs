### Contains general utility functions ###

# Function to determine recommendation level based on duplication rate
def determine_recommendation_level(dup_rate):
    if dup_rate > 0.7:
        return 'high'
    elif dup_rate > 0.3:
        return 'warning'
    else:
        return 'info'

# Denormalize a dictionary with nested dictionaries
def denormalize(data):
    """
    Denormalize a dictionary with nested dictionaries
    """
    denormalized = {}
    for index, content in data.items():
        if isinstance(content, dict):
            for inner_key, inner_value in content.items():
                new_key = f"{index}_{inner_key.lower()}"
                denormalized[new_key] = inner_value
        else:
            denormalized[index] = content
    return denormalized
