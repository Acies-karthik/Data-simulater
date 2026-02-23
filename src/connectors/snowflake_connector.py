import os
from src.connectors.base import BaseConnector

class SnowflakeConnector(BaseConnector):
    """
    Live push connector to Snowflake Data Cloud using PySpark native tools.
    Native to Databricks environments (`format("snowflake")`).
    """
    def __init__(self, **kwargs):
        self.options = {
            "sfUrl": os.environ.get("SF_ACCOUNT", "") + ".snowflakecomputing.com",
            "sfUser": os.environ.get("SF_USER", ""),
            "sfPassword": os.environ.get("SF_PASSWORD", ""),
            "sfDatabase": os.environ.get("SF_DATABASE", ""),
            "sfSchema": os.environ.get("SF_SCHEMA", "PUBLIC"),
            "sfWarehouse": os.environ.get("SF_WAREHOUSE", "")
        }
        
    def connect(self):
        print("Snowflake connection options loaded for Spark distributed write.")
        
    def push_dataframe(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Push PySpark DataFrame using Databricks/Spark-Snowflake connector.
        """
        spark_mode = "append" if mode == "append" else "overwrite"
        table_name_upper = table_name.upper()
        print(f"Pushing Spark DataFrame to Snowflake table '{table_name_upper}' (mode: {spark_mode})...")
        
        df.write.format("snowflake") \
          .options(**self.options) \
          .option("dbtable", table_name_upper) \
          .mode(spark_mode) \
          .save()
            
        print(f"✅ Successfully written to Snowflake {table_name_upper}.")
            
    def close(self):
        pass
