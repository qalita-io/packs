### Contains general utility functions ###
import re

# Function to extract variable name from the content
def extract_variable_name(content):
    # Regular expression pattern to extract variable name
    pattern = r"^(.*?)\s+has"
    match = re.search(pattern, content)
    if match:
        return match.group(1)  # Return the found variable name
    return ""  # Return empty string if no match found

def round_if_numeric(value, decimals=2):
    try:
        # Convert to a float and round
        rounded_value = round(float(value), decimals)
        # If the rounded value is an integer, convert it to an int
        if rounded_value.is_integer():
            return str(int(rounded_value))
        # Otherwise, format it as a string with two decimal places
        return "{:.2f}".format(rounded_value)
    except (ValueError, TypeError):
        # Return the original value if it's not a number
        return str(value)

# Function to extract percentage and determine level
def determine_level(content):
    """
    Function to extract percentage and determine level
    """
    # Find percentage value in the string
    match = re.search(r"(\d+(\.\d+)?)%", content)
    if match:
        percentage = float(match.group(1))
        # Determine level based on percentage
        if 0 <= percentage <= 70:
            return "info"
        elif 71 <= percentage <= 90:
            return "warning"
        elif 91 <= percentage <= 100:
            return "high"
    return "info"  # Default level if no percentage is found


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

