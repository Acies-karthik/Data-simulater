import os
from src.connectors.base import BaseConnector
from sqlalchemy import create_engine
import oracledb

class OciAdbConnector(BaseConnector):
    """
    Connector for OCI Autonomous Database using mTLS.
    Uses oracledb thick/thin mode via SQLAlchemy.
    """
    def __init__(self):
        self.user = os.environ.get("OCI_ADB_USER")
        self.password = os.environ.get("OCI_ADB_PASSWORD")
        self.dsn = os.environ.get("OCI_ADB_DSN")
        self.wallet_dir = os.environ.get("OCI_ADB_WALLET_DIR")
        self.wallet_password = os.environ.get("OCI_ADB_WALLET_PASSWORD")
        
        if not all([self.user, self.password, self.dsn, self.wallet_dir]):
            raise ValueError(
                "OCI Autonomous Database connection requires the following environment variables: "
                "OCI_ADB_USER, OCI_ADB_PASSWORD, OCI_ADB_DSN, OCI_ADB_WALLET_DIR"
            )
            
        # SQLAlchemy URL format for oracledb
        self.uri = f"oracle+oracledb://{self.user}:{self.password}@{self.dsn}"
        self.engine = None
        
    def connect(self):
        print(f"Connecting to OCI Autonomous Database ({self.dsn}) via SQLAlchemy + oracledb...")
        
        # We pass wallet configuration to oracledb through SQLAlchemy's connect_args
        # In thin mode (default), wallet_location specifies the dir containing cwallet.sso
        connect_args = {
            "config_dir": self.wallet_dir,
            "wallet_location": self.wallet_dir
        }
        if self.wallet_password:
            connect_args["wallet_password"] = self.wallet_password
            
        self.engine = create_engine(
            self.uri,
            connect_args=connect_args
        )
        
        # Test connection
        with self.engine.connect() as conn:
            pass
        print(f"OCI Autonomous Database connection established.")
        
    def push_dataframe(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Pull PySpark DataFrame to local Pandas and push to Autonomous Database.
        """
        sql_mode = "append" if mode == "append" else "replace"
        print(f"Collecting Spark DataFrame to Pandas and pushing to OCI table '{table_name}' (mode: {sql_mode})...")
        
        # Convert PySpark df to Pandas
        pdf = df.toPandas()
        
        # Note: Oracle schema is usually uppercase. We will use upper-cased schema name.
        schema_name = "SAMPLE_DATASETS"
        
        from sqlalchemy.dialects.oracle import BINARY_DOUBLE
        import numpy as np
        
        dtype_mapping = {}
        for col, dtype in pdf.dtypes.items():
            if np.issubdtype(dtype, np.floating):
                dtype_mapping[col] = BINARY_DOUBLE()
        
        # In Oracle, table names and columns should typically be short and uppercase.
        # SQLAlchemy and pandas will handle the inserts, but it's often safer to lowercase or uppercase
        # We leave it as is, but create the user schema if needed (requires admin privileges, so we assume it exists
        # or we try to create it if possible, though creating schema in Oracle is creating a user. 
        # So we'll skip CREATE SCHEMA for Oracle and just specify the schema for table creation if we have access).
        
        # Let's try to write to it directly. If the user doesn't have privileges to the schema, it will fail.
        # But per instructions, push to sample_datasets schema.
        try:
            pdf.to_sql(
                name=table_name.lower(), # typically oracle prefers lowercase for sqlalchemy/pandas to quote or uppercase
                con=self.engine,
                schema=schema_name,
                if_exists=sql_mode,
                index=False,
                chunksize=1000,
                dtype=dtype_mapping
            )
            print(f"Successfully written {len(pdf)} rows to {schema_name}.{table_name}.")
        except Exception as e:
            print(f"Error pushing to Oracle: {e}")
            print(f"Trying to write to default schema instead...")
            # Fallback to default schema if SAMPLE_DATASETS doesn't exist or isn't accessible
            pdf.to_sql(
                name=table_name.lower(),
                con=self.engine,
                if_exists=sql_mode,
                index=False,
                chunksize=1000,
                dtype=dtype_mapping
            )
            print(f"Successfully written {len(pdf)} rows to default schema table {table_name}.")

    def close(self):
        if self.engine:
            self.engine.dispose()
