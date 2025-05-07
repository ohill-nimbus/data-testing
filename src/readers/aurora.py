"""Read data from AWS ADurora and RDS databases."""

import pandas as pd


def get_dev() -> pd.DataFrame:
    """Get a sample DataFrame for development purposes.

    Returns:
        pd.DataFrame: A DataFrame with sample data.
    """
    df = pd.DataFrame({
        "A": [1, 2, 3, 4],
        "B": [5, 6, 7, 8],
        "C": [9, 10, 11, 12],
        "D": [13, 14, 15, 16],
    })
    return df
