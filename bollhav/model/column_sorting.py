import re

patterns = [
    r"^_data_modified$",
    r"^_metadata_modified$",
    r"^_",  # any other _ prefixed columns
]


def sort_columns(columns: list[str], patterns: list[str] = patterns) -> list[str]:
    def priority(col: str) -> tuple[int, str]:
        for i, pattern in enumerate(patterns):
            if re.match(pattern, col):
                return (i, col)
        return (len(patterns), col)

    return sorted(columns, key=priority)
