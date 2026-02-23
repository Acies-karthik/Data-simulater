import numpy as np
import pandas as pd

class DataQualityInjector:
    """
    Handles generation of mathematically realistic skewed data distributions and 
    injection of Data Quality anomalies for downstream testing.
    """
    
    @staticmethod
    def generate_financial_numeric(n):
        """Generates realistic normal distributions for financials."""
        vals = np.random.normal(loc=50.0, scale=15.0, size=n)
        return [max(1.0, round(v, 2)) for v in vals]
        
    @staticmethod
    def generate_age_numeric(n):
        """Generates realistic normal distributions for age."""
        vals = np.random.normal(loc=35, scale=10, size=n)
        return [max(18, min(100, int(v))) for v in vals]
        
    @staticmethod
    def generate_rating_numeric(n):
        """Generates skewed reviews/ratings."""
        return np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.05, 0.1, 0.4, 0.4], size=n)
        
    @staticmethod
    def generate_status_category(n):
        """Generates pareto distribution statuses for business logic SLA testing."""
        return np.random.choice(["Completed", "Completed", "Completed", "Pending", "Failed"], size=n)
        
    @staticmethod
    def inject_anomalies(pdf_out: pd.DataFrame, anomaly_rate: float) -> pd.DataFrame:
        """
        Randomly drops NULLs into the generated batch across non-primary key columns
        to test downstream completeness and resilient handling.
        """
        if anomaly_rate <= 0:
            return pdf_out
            
        mask = np.random.rand(*pdf_out.shape) < anomaly_rate
        if pdf_out.shape[1] > 0:
             mask[:, 0] = False # don't corrupt primary key
        return pdf_out.where(~mask, None)
