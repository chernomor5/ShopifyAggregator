from datetime import datetime

def parse_iso_datetime(dt_str):
    """
    Parses an ISO 8601 datetime string, replacing 'Z' with UTC offset.
    """
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def format_datetime(dt):
    """
    Formats a datetime object into a standard string.
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def normalize_pipe_string(s):
    """
    Normalizes a pipe-separated string by removing whitespace,
    removing duplicates, and sorting the items.
    """
    return '|'.join(sorted(set(part.strip() for part in s.split('|') if part)))
