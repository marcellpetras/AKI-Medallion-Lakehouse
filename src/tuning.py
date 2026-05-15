import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GroupKFold, RandomizedSearchCV, cross_validate
from sklearn.metrics import average_precision_score, precision_recall_curve
from xgboost import XGBClassifier
from scipy.stats import uniform, randint

AUPRC_SCORER = "average_precision"


# ─── Cross-Validation ──────────────────────────────────────────────────────────

def cross_validate_model(
    model,
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    n_folds: int = 5,
) -> pd.DataFrame:
    """
    Run GroupKFold cross-validation and return per-fold AUPRC and AUROC.

    Parameters
    ----------
    model : sklearn-compatible estimator
        The model to evaluate (unfitted).
    X : pd.DataFrame
        Feature matrix.
    y : pd.Series
        Binary target vector.
    groups : pd.Series
        Group labels (stay_id) for GroupKFold splitting.
    n_folds : int
        Number of folds (default 5).

    Returns
    -------
    pd.DataFrame
        One row per fold with columns: fold, auprc, auroc.
    """
    gkf = GroupKFold(n_splits=n_folds)

    scoring = {
        "auprc": AUPRC_SCORER,
        "auroc": "roc_auc",
    }

    cv_results = cross_validate(
        model, X, y,
        cv=gkf,
        groups=groups,
        scoring=scoring,
        return_train_score=False,
        n_jobs=-1,
    )

    results = pd.DataFrame({
        "fold": range(1, n_folds + 1),
        "auprc": cv_results["test_auprc"],
        "auroc": cv_results["test_auroc"],
    })

    print(f"Cross-Validation ({n_folds}-fold GroupKFold):")
    print(f"  AUPRC: {results['auprc'].mean():.4f} ± {results['auprc'].std():.4f}")
    print(f"  AUROC: {results['auroc'].mean():.4f} ± {results['auroc'].std():.4f}")

    return results


# ─── Hyperparameter Tuning ──────────────────────────────────────────────────────

def tune_logistic_regression(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    n_iter: int = 20,
    n_folds: int = 5,
    random_state: int = 42,
) -> dict:
    """
    Tune Logistic Regression hyperparameters using RandomizedSearchCV
    with GroupKFold and AUPRC scoring.

    Parameters
    ----------
    X, y, groups : training data and group labels.
    n_iter : int
        Number of random parameter combinations to try.
    n_folds : int
        Number of CV folds.
    random_state : int
        Random seed.

    Returns
    -------
    dict
        Contains 'best_params', 'best_score', 'cv_results', and 'best_estimator'.
    """
    param_distributions = {
        "C": uniform(loc=0.001, scale=100),       
        # sklearn 1.8+: use l1_ratio instead of penalty
        # l1_ratio=0 → pure L2 (Ridge), l1_ratio=1 → pure L1 (Lasso)
        "l1_ratio": uniform(loc=0, scale=1),       # 0.0 – 1.0 (ElasticNet continuum)
        "solver": ["saga"],                          # saga supports elasticnet
    }

    base_model = LogisticRegression(
        class_weight="balanced",
        penalty="elasticnet",      # required when using l1_ratio with saga
        max_iter=2000,
        random_state=random_state,
    )

    search = RandomizedSearchCV(
        base_model,
        param_distributions,
        n_iter=n_iter,
        cv=GroupKFold(n_splits=n_folds),
        scoring=AUPRC_SCORER,
        random_state=random_state,
        n_jobs=-1,
        refit=True,
    )

    search.fit(X, y, groups=groups)

    print(f"LR Tuning Complete — Best AUPRC: {search.best_score_:.4f}")
    print(f"  Best params: {search.best_params_}")

    return {
        "best_params": search.best_params_,
        "best_score": search.best_score_,
        "cv_results": pd.DataFrame(search.cv_results_),
        "best_estimator": search.best_estimator_,
    }


def tune_xgboost(
    X: pd.DataFrame,
    y: pd.Series,
    groups: pd.Series,
    n_iter: int = 30,
    n_folds: int = 5,
    random_state: int = 42,
) -> dict:
    """
    Tune XGBoost hyperparameters using RandomizedSearchCV
    with GroupKFold and AUPRC scoring.

    Parameters
    ----------
    X, y, groups : training data and group labels.
    n_iter : int
        Number of random parameter combinations to try.
    n_folds : int
        Number of CV folds.
    random_state : int
        Random seed.

    Returns
    -------
    dict
        Contains 'best_params', 'best_score', 'cv_results', and 'best_estimator'.
    """
    neg_count = (y == 0).sum()
    pos_count = (y == 1).sum()

    param_distributions = {
        "n_estimators": randint(100, 500),
        "max_depth": randint(3, 10),
        "learning_rate": uniform(loc=0.01, scale=0.29),   # actual value will be [loc, loc + scale]
        "subsample": uniform(loc=0.6, scale=0.4),         
        "colsample_bytree": uniform(loc=0.5, scale=0.5),  
        "min_child_weight": randint(1, 10),
    }

    base_model = XGBClassifier(
        scale_pos_weight=neg_count / max(pos_count, 1),
        eval_metric="aucpr",
        random_state=random_state,
        n_jobs=-1,
    )

    search = RandomizedSearchCV(
        base_model,
        param_distributions,
        n_iter=n_iter,
        cv=GroupKFold(n_splits=n_folds),
        scoring=AUPRC_SCORER,
        random_state=random_state,
        n_jobs=1,          
        refit=True,
    )

    search.fit(X, y, groups=groups)

    print(f"XGB Tuning Complete — Best AUPRC: {search.best_score_:.4f}")
    print(f"  Best params: {search.best_params_}")

    return {
        "best_params": search.best_params_,
        "best_score": search.best_score_,
        "cv_results": pd.DataFrame(search.cv_results_),
        "best_estimator": search.best_estimator_,
    }


# ─── Threshold Optimisation ────────────────────────────────────────────────────

def find_optimal_threshold(
    y_true: np.ndarray,
    y_prob: np.ndarray,
    strategy: str = "f1",
    min_recall: float = 0.5,
) -> dict:
    """
    Find the optimal decision threshold from the Precision-Recall curve.

    Parameters
    ----------
    y_true : array-like
        Ground truth binary labels.
    y_prob : array-like
        Predicted probabilities for the positive class.
    strategy : str
        'f1' — maximise F1 score.
        'recall' — find the highest precision threshold that achieves >= min_recall.
    min_recall : float
        Only used when strategy='recall'. Target minimum recall.

    Returns
    -------
    dict
        Contains 'threshold', 'precision', 'recall', 'f1' at the optimal point.
    """
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)

    # precision_recall_curve returns n+1 precision/recall values but n thresholds
    precision = precision[:-1]
    recall = recall[:-1]

    if strategy == "f1":
        f1_scores = 2 * (precision * recall) / np.maximum(precision + recall, 1e-8)
        best_idx = np.argmax(f1_scores)
    elif strategy == "recall":
        valid_mask = recall >= min_recall
        if valid_mask.sum() == 0:
            print(f"Warning: No threshold achieves recall >= {min_recall}. Using F1 fallback.")
            f1_scores = 2 * (precision * recall) / np.maximum(precision + recall, 1e-8)
            best_idx = np.argmax(f1_scores)
        else:
            valid_precisions = np.where(valid_mask, precision, -1)
            best_idx = np.argmax(valid_precisions)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    best_threshold = thresholds[best_idx]
    best_precision = precision[best_idx]
    best_recall = recall[best_idx]
    best_f1 = 2 * (best_precision * best_recall) / max(best_precision + best_recall, 1e-8)

    print(f"Optimal threshold ({strategy}): {best_threshold:.4f}")
    print(f"  Precision: {best_precision:.4f}  |  Recall: {best_recall:.4f}  |  F1: {best_f1:.4f}")

    return {
        "threshold": float(best_threshold),
        "precision": float(best_precision),
        "recall": float(best_recall),
        "f1": float(best_f1),
    }
