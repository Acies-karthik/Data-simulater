import os
import logging
import pandas as pd
import numpy as np
from src.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class SnowflakeConnector(BaseConnector):
    """
    Live push connector to Snowflake Data Cloud using PySpark native tools or
    snowflake-connector-python / write_pandas fallback.
    """
    def __init__(self, **kwargs):
        account = os.environ.get("SF_ACCOUNT", "")
        if account and not account.endswith(".snowflakecomputing.com"):
            url = account + ".snowflakecomputing.com"
        else:
            url = account

        self.account = account.replace(".snowflakecomputing.com", "")
        self.user = os.environ.get("SF_USER", "")
        self.password = os.environ.get("SF_PASSWORD", "")
        self.database = os.environ.get("SF_DATABASE", "")
        self.schema = os.environ.get("SF_SCHEMA", "PUBLIC")
        self.warehouse = os.environ.get("SF_WAREHOUSE", "")

        self.options = {
            "sfUrl": url,
            "sfUser": self.user,
            "sfPassword": self.password,
            "sfDatabase": self.database,
            "sfSchema": self.schema,
            "sfWarehouse": self.warehouse
        }
        self.conn = None
        
    def connect(self):
        print(f"Snowflake connector initialized for account '{self.account}', DB '{self.database}', Schema '{self.schema}'.")

    def _get_python_conn(self):
        if self.conn is None or self.conn.is_closed():
            import snowflake.connector
            self.conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
        return self.conn
        
    def push_dataframe(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Push DataFrame to Snowflake table.
        Tries Spark native writer first; falls back to snowflake.connector + write_pandas.
        """
        spark_mode = "append" if mode == "append" else "overwrite"
        table_name_upper = table_name.upper()
        print(f"Pushing DataFrame to Snowflake table '{table_name_upper}' (mode: {spark_mode})...")

        try:
            df.write.format("snowflake") \
              .options(**self.options) \
              .option("dbtable", table_name_upper) \
              .mode(spark_mode) \
              .save()
            print(f"✅ Successfully written to Snowflake '{table_name_upper}' via Spark Snowflake connector.")
        except Exception as e:
            if "ClassNotFoundException" in str(e) or "snowflake" in str(e).lower() or "java.lang" in str(e).lower():
                print(f"Spark Snowflake connector not present, using python snowflake.connector write_pandas...")
                try:
                    from snowflake.connector.pandas_tools import write_pandas
                    conn = self._get_python_conn()

                    if hasattr(df, "toPandas"):
                        pdf = df.toPandas()
                    else:
                        pdf = df

                    # Ensure column names are uppercase for Snowflake
                    pdf.columns = [c.upper() for c in pdf.columns]

                    # Sanitize non-numeric columns so nan/NaN values become python None (SQL NULL)
                    for col in pdf.columns:
                        if not pd.api.types.is_numeric_dtype(pdf[col]):
                            pdf[col] = pdf[col].apply(lambda x: None if (pd.isna(x) or str(x).lower() == "nan") else x)

                    overwrite_flag = (mode != "append")
                    success, nchunks, nrows, _ = write_pandas(
                        conn,
                        pdf,
                        table_name_upper,
                        auto_create_table=True,
                        overwrite=overwrite_flag,
                        database=self.database if self.database else None,
                        schema=self.schema if self.schema else None
                    )
                    print(f"✅ Successfully written {nrows} rows to Snowflake table '{table_name_upper}' via write_pandas.")
                except Exception as py_err:
                    print(f"❌ Failed writing to Snowflake table '{table_name_upper}': {py_err}")
                    raise py_err
            else:
                raise e

    def close(self):
        if self.conn and not self.conn.is_closed():
            try:
                self.conn.close()
            except Exception:
                pass

