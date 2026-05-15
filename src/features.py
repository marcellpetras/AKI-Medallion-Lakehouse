import pandas as pd
from sklearn.preprocessing import StandardScaler



ID_COLUMNS = ["stay_id", "patient_id", "hr_timestamp"]

KDIGO_COLUMN = "is_aki_now"

# The prediction target
TARGET_COLUMN = "aki_within_6h"

# Ordered list of numeric features for the ML models
NUMERIC_FEATURES = [
    "hr_index",
    "creatinine",
    "bun",
    "heart_rate",
    "map",
    "uo_hourly",
    "uo_ml_kg_hr",
    "age",
    "admission_weight",
    "hours_since_last_cr",
    "hours_since_last_bun",
    "hours_since_last_hr",
    "hours_since_last_map",
]

# Binary features (already 0/1 in Gold table)
BINARY_FEATURES = [
    "history_ckd",
    "history_diabetes",
    "history_chf",
    "history_sepsis",
]


def encode_gender(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the 'gender' string column into a binary 'is_male' column.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing a 'gender' column.
    
    Returns
    -------
    pd.DataFrame
        DataFrame with 'gender' replaced by 'is_male' (1 = male, 0 = female).
    """
    df = df.copy()
    df["is_male"] = (df["gender"] == "male").astype(int)
    df = df.drop(columns=["gender"])
    return df


def get_feature_columns() -> list[str]:
    """Return the ordered list of all feature column names (after encoding)."""
    return NUMERIC_FEATURES + BINARY_FEATURES + ["is_male"]


def prepare_features(
    df: pd.DataFrame,
    scale: bool = False,
    scaler: StandardScaler | None = None,
):
    """
    Transform a Gold DataFrame into ML-ready feature matrix and target vector.
    
    Parameters
    ----------
    df : pd.DataFrame
        A train or test DataFrame from data_loader.train_test_split_by_stay().
    scale : bool
        If True, apply StandardScaler to numeric features (required for Logistic Regression).
    scaler : StandardScaler or None
        If provided, use this fitted scaler (for test data). If None and scale=True,
        fit a new scaler on the data (for training data).
    
    Returns
    -------
    tuple[pd.DataFrame, pd.Series, StandardScaler | None]
        (X, y, scaler) — the feature matrix, target vector, and fitted scaler (if applicable).
    """
    df = encode_gender(df)
    
    feature_cols = get_feature_columns()
    X = df[feature_cols].copy()
    y = df[TARGET_COLUMN].copy()
    
    X = X.fillna(0)
    
    fitted_scaler = None
    if scale:
        if scaler is None:
            fitted_scaler = StandardScaler()
            X[NUMERIC_FEATURES] = fitted_scaler.fit_transform(X[NUMERIC_FEATURES])
        else:
            fitted_scaler = scaler
            X[NUMERIC_FEATURES] = fitted_scaler.transform(X[NUMERIC_FEATURES])
    
    return X, y, fitted_scaler
