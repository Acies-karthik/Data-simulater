from abc import ABC, abstractmethod
import pandas as pd

class BaseConnector(ABC):
    """
    Abstract Base Class for all Database Connectors.
    Defines the contract for pushing generated synthetic data to a target system.
    """
    
    @abstractmethod
    def connect(self):
        """Establish connection to the target system."""
        pass
        
    @abstractmethod
    def push_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = "append", partition_date: str = None):
        """
        Push a pandas DataFrame to the target table.
        Args:
            df: The pandas DataFrame containing the generated rows.
            table_name: The destination table name.
            mode: 'append' (incremental load) or 'replace' (initial load).
            partition_date: Optional Hive-partition date (YYYY-MM-DD) for data lakes.
        """
        pass
        
    @abstractmethod
    def close(self):
        """Close connection to the target system."""
        pass
