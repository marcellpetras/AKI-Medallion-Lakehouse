import duckdb
import pandas as pd
import os
from sklearn.model_selection import GroupShuffleSplit


# Default path matches the pipeline config for local development
DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(__file__), "..", "lakehouse_db", "aki_lakehouse.db"
)
DB_PATH = os.getenv("DB_PATH", os.path.abspath(DEFAULT_DB_PATH))


def load_gold_table(db_path: str = DB_PATH) -> pd.DataFrame:
    """
    Load the full gold_hourly_clinical table from DuckDB into a pandas DataFrame.
    
    Parameters
    ----------
    db_path : str
        Path to the DuckDB database file.
    
    Returns
    -------
    pd.DataFrame
        The complete gold_hourly_clinical table.
    """
    conn = duckdb.connect(db_path, read_only=True)
    df = conn.execute("SELECT * FROM gold_hourly_clinical").df()
    conn.close()
    
    print(f"Loaded {len(df):,} hourly samples across {df['stay_id'].nunique():,} stays.")
    return df


def train_test_split_by_stay(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42,
):
    """
    Split the dataset into train and test sets, grouped by stay_id.
    
    This ensures that all hourly observations from a single ICU stay
    appear exclusively in either the train or test set, never both.
    
    Parameters
    ----------
    df : pd.DataFrame
        The gold_hourly_clinical DataFrame.
    test_size : float
        Fraction of stays to reserve for testing (default 0.2 = 20%).
    random_state : int
        Random seed for reproducibility.
    
    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (train_df, test_df) — full DataFrames including all columns.
    """
    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    groups = df["stay_id"]
    
    train_idx, test_idx = next(splitter.split(df, groups=groups))
    
    train_df = df.iloc[train_idx].reset_index(drop=True)
    test_df = df.iloc[test_idx].reset_index(drop=True)
    
    print(f"Train: {len(train_df):,} samples ({train_df['stay_id'].nunique():,} stays)")
    print(f"Test:  {len(test_df):,} samples ({test_df['stay_id'].nunique():,} stays)")
    
    overlap = set(train_df["stay_id"]) & set(test_df["stay_id"])
    assert len(overlap) == 0, f"Data leakage! {len(overlap)} stays appear in both sets."
    
    return train_df, test_df
