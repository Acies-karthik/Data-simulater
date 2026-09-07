import os
from typing import Literal
from src.connectors.base import BaseConnector

class FileConnector(BaseConnector):
    """
    Export PySpark DataFrame to scalable file formats directly via Spark.
    """
    
    def __init__(self, output_dir: str = "output_data", format: Literal["csv", "json", "parquet"] = "csv"):
        self.output_dir = output_dir
        self.format = format
        os.makedirs(self.output_dir, exist_ok=True)
        
    def connect(self):
        pass
        
    def push_dataframe(self, df, table_name: str, mode: str = "append", partition_date: str = None):
        """
        Exports DataFrame to a single file directly in the format directory.
        e.g. output_data/csv/users.csv
        """
        import pandas as pd
        out_dir = os.path.join(self.output_dir, self.format)
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(out_dir, f"{table_name}.{self.format}")
        
        print(f"Pushing single file to {file_path}...")
        
        # Convert to Pandas for single-file output
        pdf = df.toPandas()
        
        pandas_mode = "a" if mode == "append" else "w"
        header = True if pandas_mode == "w" or not os.path.exists(file_path) else False
        
        if self.format == "csv":
            pdf.to_csv(file_path, mode=pandas_mode, header=header, index=False, na_rep="NULL")
        elif self.format == "json":
            pdf.to_json(file_path, orient="records", lines=True, mode=pandas_mode)
        elif self.format == "parquet":
            if pandas_mode == "a" and os.path.exists(file_path):
                existing_pdf = pd.read_parquet(file_path)
                pdf = pd.concat([existing_pdf, pdf], ignore_index=True)
            pdf.to_parquet(file_path, index=False)
        else:
            raise ValueError(f"Unsupported format {self.format} for FileConnector.")
            
        print(f"✅ Successfully exported {len(pdf)} rows to {file_path}.")
            
    def close(self):
        pass

