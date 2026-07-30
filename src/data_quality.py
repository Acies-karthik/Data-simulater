import os
import json
import numpy as np
import pandas as pd


def load_rules_config(path: str = None) -> dict:
    """
    Loads the rules_config.json toggle file.
    Falls back to RULES_CONFIG_PATH env var, then to 'rules_config.json'.
    Returns an empty dict if the file is not found (all rules off).
    """
    resolved = path or os.environ.get("RULES_CONFIG_PATH", "rules_config.json")
    try:
        with open(resolved, "r") as f:
            cfg = json.load(f)
        # Strip comment keys
        return {k: v for k, v in cfg.items() if not k.startswith("_")}
    except FileNotFoundError:
        print(f"Warning: rules_config.json not found at '{resolved}'. All rules disabled.")
        return {}


class DataQualityInjector:
    """
    Handles generation of mathematically realistic skewed data distributions and
    injection of Data Quality anomalies for downstream testing.

    Rules are controlled via rules_config.json. Each rule key maps to a boolean
    toggle that enables or disables the corresponding anomaly injection.

    Active rules:
      null_count_on_numerical  — Randomly injects NULLs into numeric columns (≈30% rate).
      below_zero               — Forces some numeric values below zero.
      zero_count_check         — Injects zero values into numeric columns.
      total_value_diff_on_numerical_columns — Introduces outlier spikes in numeric columns.
    """

    def __init__(self, rules: dict = None):
        self.rules = rules if rules is not None else {}

    def _rule(self, key: bool) -> bool:
        return bool(self.rules.get(key, False))

    # ------------------------------------------------------------------
    # Distribution generators (always used regardless of rules)
    # ------------------------------------------------------------------

    @staticmethod
    def generate_financial_numeric(n):
        """Generates realistic normal distributions for financials."""
        vals = np.random.normal(loc=50.0, scale=15.0, size=n)
        return np.array([max(1.0, round(v, 2)) for v in vals], dtype=np.float64)

    @staticmethod
    def generate_age_numeric(n):
        """Generates realistic normal distributions for age."""
        vals = np.random.normal(loc=35, scale=10, size=n)
        return np.array([max(18, min(100, int(v))) for v in vals], dtype=np.int64)

    @staticmethod
    def generate_rating_numeric(n):
        """Generates skewed reviews/ratings."""
        return np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.05, 0.1, 0.4, 0.4], size=n).astype(np.int64)

    @staticmethod
    def generate_status_category(n):
        """Generates pareto distribution statuses for business logic SLA testing."""
        return np.random.choice(["Completed", "Completed", "Completed", "Pending", "Failed"], size=n)

    # ------------------------------------------------------------------
    # Rule-driven anomaly injection
    # ------------------------------------------------------------------

    def inject_anomalies(self, pdf_out: pd.DataFrame, anomaly_rate: float = 0.05) -> pd.DataFrame:
        """
        Applies enabled rule injections in sequence.

        Rule: null_count_on_numerical
          Randomly drops NULLs into numeric columns (≤30% of rows) to test
          downstream completeness and resilience.

        Rule: below_zero
          Forces a small fraction of numeric values below zero.

        Rule: zero_count_check
          Injects zero values into numeric columns.

        Rule: total_value_diff_on_numerical_columns
          Introduces outlier spike values into numeric columns.
        """
        numeric_cols = [
            c for c in pdf_out.columns
            if pd.api.types.is_numeric_dtype(pdf_out[c]) and c != pdf_out.columns[0]
        ]

        # --- Rule: null_count_on_numerical ---
        if self._rule("null_count_on_numerical") and anomaly_rate > 0:
            null_rate = min(anomaly_rate, 0.30)  # cap at 30% per rule definition
            mask = np.random.rand(len(pdf_out), len(numeric_cols)) < null_rate
            for i, col in enumerate(numeric_cols):
                pdf_out.loc[mask[:, i], col] = None

        # --- Rule: below_zero ---
        if self._rule("below_zero"):
            below_zero_rate = 0.05  # 5% of rows get a negative value
            for col in numeric_cols:
                idxs = np.where(np.random.rand(len(pdf_out)) < below_zero_rate)[0]
                if len(idxs):
                    pdf_out.iloc[idxs, pdf_out.columns.get_loc(col)] = \
                        pdf_out.iloc[idxs][col].abs() * -1

        # --- Rule: zero_count_check ---
        if self._rule("zero_count_check"):
            zero_rate = 0.05
            for col in numeric_cols:
                idxs = np.where(np.random.rand(len(pdf_out)) < zero_rate)[0]
                if len(idxs):
                    pdf_out.iloc[idxs, pdf_out.columns.get_loc(col)] = 0

        # --- Rule: total_value_diff_on_numerical_columns ---
        if self._rule("total_value_diff_on_numerical_columns"):
            spike_rate = 0.03
            for col in numeric_cols:
                idxs = np.where(np.random.rand(len(pdf_out)) < spike_rate)[0]
                if len(idxs):
                    # Multiply by a large factor to create an outlier spike
                    pdf_out.iloc[idxs, pdf_out.columns.get_loc(col)] = \
                        pdf_out.iloc[idxs][col] * np.random.uniform(10, 30)

        return pdf_out
