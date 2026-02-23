import os
from src.connectors.base import BaseConnector
from sqlalchemy import create_engine

class PostgresConnector(BaseConnector):
    """
    Live push connector to external PostgreSQL database.
    Uses SQLAlchemy + psycopg2 for reliable local insert of generated batches.
    """
    def __init__(self, connection_uri: str = None):
        uri = connection_uri or os.environ.get("POSTGRES_URI")
        if not uri:
            raise ValueError("POSTGRES_URI environment variable required. Format: postgresql://user:password@host:port/dbname")
            
        # Ensure URI is compatible with SQLAlchemy
        if uri.startswith("jdbc:postgresql://"):
            # Very basic conversion for ease of use if user kept the old format
            self.uri = uri.replace("jdbc:postgresql://", "postgresql://")
        else:
            self.uri = uri
            
        self.engine = None
            
    def connect(self):
        print(f"Connecting to Postgres using SQLAlchemy...")
        self.engine = create_engine(self.uri)
        # Test connection
        with self.engine.connect() as conn:
            pass
        print(f"Postgres connection established.")
        
    def push_dataframe(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Pull PySpark DataFrame to local Pandas and push to Postgres.
        *Note: For absolute massive Databricks runs, natively configure Spark JDBC instead of this.*
        """
        sql_mode = "append" if mode == "append" else "replace"
        print(f"Collecting Spark DataFrame to Pandas and pushing to Postgres table '{table_name}' (mode: {sql_mode})...")
        
        # Convert PySpark df to Pandas
        pdf = df.toPandas()
        
        # Ensure schema exists, then push to postgres
        with self.engine.begin() as conn:
            conn.exec_driver_sql('CREATE SCHEMA IF NOT EXISTS "Sample_datasets";')
            
        pdf.to_sql(table_name, con=self.engine, schema="Sample_datasets", if_exists=sql_mode, index=False, chunksize=1000)
          
        print(f"Successfully written {len(pdf)} rows to {table_name}.")
            
    def close(self):
        if self.engine:
            self.engine.dispose()
