import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_recall_curve,
    roc_curve,
    classification_report,
    confusion_matrix,
)


def compute_metrics(y_true, y_prob, threshold: float = 0.5) -> dict:
    """
    Compute a standard suite of classification metrics.
    
    Parameters
    ----------
    y_true : array-like
        Ground truth binary labels (0 or 1).
    y_prob : array-like
        Predicted probabilities for the positive class.
    threshold : float
        Decision threshold for converting probabilities to binary predictions.
    
    Returns
    -------
    dict
        Dictionary with AUPRC, AUROC, precision, recall, F1, and confusion matrix.
    """
    y_pred = (np.array(y_prob) >= threshold).astype(int)
    y_true = np.array(y_true)
    
    report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
    cm = confusion_matrix(y_true, y_pred)
    
    return {
        "auprc": average_precision_score(y_true, y_prob),
        "auroc": roc_auc_score(y_true, y_prob),
        "precision": report["1"]["precision"],
        "recall": report["1"]["recall"],
        "f1": report["1"]["f1-score"],
        "confusion_matrix": cm,
        "threshold": threshold,
    }


def compute_metrics_at_threshold(y_true, y_prob, threshold: float) -> dict:
    """
    Compute classification metrics at a specific (tuned) decision threshold.
    
    This is a convenience wrapper around compute_metrics that makes it explicit
    in notebooks that we are using an optimised threshold rather than the default.
    
    Parameters
    ----------
    y_true : array-like
        Ground truth binary labels.
    y_prob : array-like
        Predicted probabilities.
    threshold : float
        Optimised decision threshold (from tuning.find_optimal_threshold).
    
    Returns
    -------
    dict
        Same structure as compute_metrics.
    """
    return compute_metrics(y_true, y_prob, threshold=threshold)


def compute_curves(y_true, y_prob) -> dict:
    """
    Compute PR and ROC curves for plotting.
    
    Returns
    -------
    dict
        Contains 'pr' (precision, recall, thresholds) and 'roc' (fpr, tpr, thresholds).
    """
    pr_precision, pr_recall, pr_thresholds = precision_recall_curve(y_true, y_prob)
    fpr, tpr, roc_thresholds = roc_curve(y_true, y_prob)
    
    return {
        "pr": {"precision": pr_precision, "recall": pr_recall, "thresholds": pr_thresholds},
        "roc": {"fpr": fpr, "tpr": tpr, "thresholds": roc_thresholds},
    }


def kdigo_baseline_predictions(test_df: pd.DataFrame) -> np.ndarray:
    """
    Generate the KDIGO rule-based baseline "predictions" for the test set.
    
    The KDIGO criteria is purely reactive: it only flags AKI at the exact moment
    the clinical thresholds are breached. It has zero predictive lead time.
    
    In our evaluation framework, the KDIGO "model" predicts aki_within_6h = 1
    whenever is_aki_now = 1. Since our gold table filters rows to hr_index < onset,
    is_aki_now should always be 0, meaning KDIGO predicts "no AKI" for every row.
    This establishes the baseline that our ML models must beat.
    
    Parameters
    ----------
    test_df : pd.DataFrame
        The test DataFrame (must contain 'is_aki_now').
    
    Returns
    -------
    np.ndarray
        Binary predictions (0 or 1) from the KDIGO baseline.
    """
    return test_df["is_aki_now"].values.astype(int)


def compute_lead_time(
    test_df: pd.DataFrame,
    y_prob: np.ndarray,
    threshold: float = 0.5,
) -> pd.DataFrame:
    """
    For stays where AKI was correctly predicted (true positives), calculate
    how many hours before the KDIGO flag the ML model raised the alarm.
    
    Parameters
    ----------
    test_df : pd.DataFrame
        Test DataFrame with 'stay_id', 'hr_index', 'aki_within_6h'.
    y_prob : np.ndarray
        Predicted probabilities from the ML model.
    threshold : float
        Decision threshold.
    
    Returns
    -------
    pd.DataFrame
        One row per stay with columns: stay_id, first_ml_alert_hr, first_aki_hr, lead_time_hours.
    """
    df = test_df[["stay_id", "hr_index", "aki_within_6h"]].copy()
    df["y_pred"] = (np.array(y_prob) >= threshold).astype(int)
    
    ml_alerts = (
        df[df["y_pred"] == 1]
        .groupby("stay_id")["hr_index"]
        .min()
        .rename("first_ml_alert_hr")
    )
    
    aki_flags = (
        df[df["aki_within_6h"] == 1]
        .groupby("stay_id")["hr_index"]
        .min()
        .rename("first_aki_flag_hr")
    )
    
    lead = pd.concat([ml_alerts, aki_flags], axis=1).dropna()
    lead["lead_time_hours"] = lead["first_aki_flag_hr"] - lead["first_ml_alert_hr"]
    
    return lead.reset_index()


def compare_models(results: dict) -> pd.DataFrame:
    """
    Create a comparison table across multiple models.
    
    Parameters
    ----------
    results : dict
        Mapping of model_name -> metrics dict (from compute_metrics).
    
    Returns
    -------
    pd.DataFrame
        Comparison table with one row per model.
    """
    rows = []
    for name, metrics in results.items():
        rows.append({
            "Model": name,
            "AUPRC": round(metrics["auprc"], 4),
            "AUROC": round(metrics["auroc"], 4),
            "Precision": round(metrics["precision"], 4),
            "Recall": round(metrics["recall"], 4),
            "F1": round(metrics["f1"], 4),
        })
    return pd.DataFrame(rows).set_index("Model")
