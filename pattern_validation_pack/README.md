# Pattern Validation Pack

Validates data formats using predefined patterns (email, UUID, IP, URL) and custom regex rules.

##  Checks Covered

- `invalid_email_format_found` / `invalid_email_format_percent`
- `invalid_uuid_format_found` / `invalid_uuid_format_percent`
- `invalid_ip4_address_format_found` / `invalid_ip6_address_format_found`
- `text_not_matching_regex_found` / `texts_not_matching_regex_percent`
- `text_not_matching_date_pattern_found`

## Built-in Pattern Types

| Type | Description |
|------|-------------|
| `email` | Standard email format |
| `uuid` | UUID/GUID format |
| `ipv4` | IPv4 address format |
| `ipv6` | IPv6 address format |
| `url` | HTTP/HTTPS URL format |
| `phone_international` | International phone number (E.164) |
| `date_iso` | ISO date format (YYYY-MM-DD) |
| `date_us` | US date format (MM/DD/YYYY) |
| `date_eu` | European date format (DD-MM-YYYY) |
| `datetime_iso` | ISO datetime format |
| `credit_card` | Credit card number format |
| `hex_color` | Hex color code (#RGB or #RRGGBB) |
| `mac_address` | MAC address format |
| `postal_code_us` | US postal code format |
| `alphanumeric` | Alphanumeric characters only |

## Configuration

```json
{
  "job": {
    "patterns": [
      {"column": "email", "type": "email"},
      {"column": "user_id", "type": "uuid"},
      {"column": "ip_address", "type": "ipv4"},
      {"column": "custom_code", "type": "regex", "regex": "^[A-Z]{2}\\d{4}$"}
    ]
  }
}
```

## Auto-Detection

If no patterns are configured, the pack will auto-detect and validate:
- Columns with "email" or "mail" in the name
- Columns with "uuid" or "guid" in the name
- Columns with "ip" and "address" in the name

## Metrics Output

- `invalid_<pattern>_format_found`: Count of invalid values
- `invalid_<pattern>_format_percent`: Percentage of invalid values
- `valid_<pattern>_percent`: Percentage of valid values
- `score`: Overall validity score (0-1)

## License

Proprietary - QALITA SAS
