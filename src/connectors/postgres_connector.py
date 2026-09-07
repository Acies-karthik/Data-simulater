import os
import pandas as pd
from urllib.parse import quote_plus
from src.connectors.base import BaseConnector
from sqlalchemy import create_engine


class PostgresConnector(BaseConnector):
    """
    Live push connector to external PostgreSQL database.
    Uses SQLAlchemy + psycopg2 for reliable local insert of generated batches.

    Connection resolution order:
      1. Explicit `connection_uri` argument
      2. POSTGRES_URI env var (full URI)
      3. Individual PG_DB_* env vars (username, password, host, port, dbname)
         Password is URL-encoded to handle special characters (e.g. '@').
    """

    def __init__(self, connection_uri: str = None):
        uri = connection_uri or os.environ.get("POSTGRES_URI")

        if not uri:
            # Build from individual env vars — URL-encode password to handle
            # special characters such as '@', '#', etc.
            user = os.environ.get("PG_DB_USERNAME")
            password = quote_plus(os.environ.get("PG_DB_PASSWORD", ""))
            host = os.environ.get("PG_DB_HOST")
            port = os.environ.get("PG_DB_PORT", "5432")
            dbname = os.environ.get("PG_DB_NAME")

            if not all([user, host, dbname]):
                raise ValueError(
                    "Postgres credentials not found. Set either POSTGRES_URI or "
                    "PG_DB_USERNAME / PG_DB_PASSWORD / PG_DB_HOST / PG_DB_PORT / PG_DB_NAME."
                )

            uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        # Normalise JDBC-style URIs
        if uri.startswith("jdbc:postgresql://"):
            uri = uri.replace("jdbc:postgresql://", "postgresql://")

        self.uri = uri

        # Schema to write into — defaults to "Sample_datasets" for backwards compat
        raw_schema = os.environ.get("PG_SCHEMA", "Sample_datasets")
        self.schema = raw_schema.strip().strip('"')

        self.engine = None

    def connect(self):
        print(f"Connecting to Postgres (schema: {self.schema})...")
        self.engine = create_engine(self.uri)
        with self.engine.connect() as conn:
            pass
        print("Postgres connection established.")

    def push_dataframe(self, df, table_name: str, mode: str = "append", **kwargs):
        """
        Pull PySpark DataFrame to local Pandas and push to Postgres.
        *Note: For absolute massive Databricks runs, natively configure Spark JDBC instead.*
        """
        sql_mode = "append" if mode == "append" else "replace"
        print(
            f"Collecting Spark DataFrame to Pandas and pushing to "
            f"Postgres table '{self.schema}.{table_name}' (mode: {sql_mode})..."
        )

        # Convert PySpark df to Pandas
        if hasattr(df, "toPandas"):
            try:
                pdf = df.toPandas()
            except Exception:
                rows = [row.asDict() for row in df.collect()]
                pdf = pd.DataFrame(rows)
        else:
            pdf = df

        # Ensure schema exists then push
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}";')

        pdf.to_sql(
            table_name,
            con=self.engine,
            schema=self.schema,
            if_exists=sql_mode,
            index=False,
            chunksize=1000,
        )

        print(f"Successfully written {len(pdf)} rows to {self.schema}.{table_name}.")

    def close(self):
        if self.engine:
            self.engine.dispose()
