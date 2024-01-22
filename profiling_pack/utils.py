import re

# Constants for determine_level function
INFO_THRESHOLD = 70
WARNING_THRESHOLD = 90
HIGH_THRESHOLD = 100


def extract_variable_name(content):
    # Regular expression pattern to extract variable name
    pattern = r"^(.*?)\s+has"
    match = re.search(pattern, content)
    return match.group(1) if match else ""


def round_if_numeric(value, decimals=2):
    try:
        # Convert to a float and round
        rounded_value = round(float(value), decimals)
        # Format it as a string with the specified number of decimal places
        return f"{rounded_value:.{decimals}f}".rstrip("0").rstrip(
            "."
        )  # Removes trailing zeros and dot if it's an integer
    except (ValueError, TypeError):
        # Return the original value if it's not a number
        return str(value)


def determine_level(content):
    match = re.search(r"(\d+(\.\d+)?)%", content)
    if match:
        percentage = float(match.group(1))
        if percentage <= INFO_THRESHOLD:
            return "info"
        elif percentage <= WARNING_THRESHOLD:
            return "warning"
        elif percentage <= HIGH_THRESHOLD:
            return "high"
    return "info"


def denormalize(data):
    denormalized = {}
    for index, content in data.items():
        if isinstance(content, dict):
            for inner_key, inner_value in content.items():
                new_key = f"{index}_{inner_key.lower()}"
                denormalized[new_key] = inner_value
        else:
            denormalized[index] = content
    return denormalized
