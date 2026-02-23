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
        Uses PySpark's highly optimized distributed native writers.
        df is a pyspark DataFrame.
        """
        spark_mode = "append" if mode == "append" else "overwrite"
        
        # Build strict Hive-style partitioned path for Incremental Data Lakes
        if partition_date:
            out_path = os.path.join(self.output_dir, table_name, f"load_date={partition_date}")
        else:
            out_path = os.path.join(self.output_dir, table_name)
            
        print(f"Pushing Spark DataFrame to {out_path} as {self.format}...")
        
        writer = df.write.mode(spark_mode)
        
        if self.format == "csv":
            writer.option("header", "true").csv(out_path)
        elif self.format == "json":
            writer.json(out_path)
        elif self.format == "parquet":
            writer.parquet(out_path)
        else:
            raise ValueError(f"Unsupported format {self.format} for Spark FileConnector.")
            
        print(f"✅ Successfully exported partition chunks to {out_path}.")
            
    def close(self):
        pass
