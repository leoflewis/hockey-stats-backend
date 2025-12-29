from datetime import datetime, date


def FormatSeason(self, date: date) -> str:
    year = date.year
    month = date.month

    if month >= 10:  # October–December
        start_year = year
        end_year = year + 1
    else:  # January–September
        start_year = year - 1
        end_year = year

    return f"{start_year}{end_year}"
