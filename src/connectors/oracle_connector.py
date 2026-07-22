import os
import logging
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.types import Numeric, Integer, String
from src.connectors.base import BaseConnector
import oracledb

logger = logging.getLogger(__name__)

class OracleConnector(BaseConnector):
    """
    Live push connector to external Oracle Database.
    Uses SQLAlchemy + python-oracledb (thick mode) to push synthetic dataset batches.
    """
    def __init__(self, connection_uri: str = None, schema: str = None, instantclient_dir: str = r"C:\oracle\instantclient_21_15"):
        self.user = os.environ.get("ORACLE_USER", "SYSTEM")
        self.password = os.environ.get("ORACLE_PASSWORD", "Preventual_Acies#06#26")
        self.host = os.environ.get("ORACLE_HOST", "92.4.85.120")
        self.port = int(os.environ.get("ORACLE_PORT", "1521"))
        self.service_name = os.environ.get("ORACLE_SERVICE_NAME", "saba_h3q_bom.preventualdbs2.vcn06021248.oraclevcn.com")
        self.schema = schema or os.environ.get("ORACLE_SCHEMA", "SAMPLE_DATASETS")
        self.instantclient_dir = instantclient_dir
        self.engine = None
        self.connection_uri = connection_uri

        # Setup Oracle client thick mode and sqlnet.ora (DISABLE_OOB=ON for ORA-12637)
        self._init_client()

    def _init_client(self):
        admin_dir = os.path.join(self.instantclient_dir, "network", "admin")
        os.makedirs(admin_dir, exist_ok=True)
        sqlnet_path = os.path.join(admin_dir, "sqlnet.ora")
        with open(sqlnet_path, "w") as f:
            f.write("SQLNET.AUTHENTICATION_SERVICES = (NONE)\nDISABLE_OOB = ON\n")

        try:
            oracledb.init_oracle_client(lib_dir=self.instantclient_dir, config_dir=admin_dir)
            logger.info(f"Oracle Instant Client initialized in thick mode from {self.instantclient_dir}.")
        except oracledb.ProgrammingError as e:
            if "already" in str(e).lower():
                pass
            else:
                logger.warning(f"Oracle client init warning: {e}")

    def connect(self):
        if not self.connection_uri:
            pwd_escaped = quote_plus(self.password)
            self.connection_uri = (
                f"oracle+oracledb://{self.user}:{pwd_escaped}@{self.host}:{self.port}"
                f"/?service_name={self.service_name}"
            )
        
        logger.info(f"Connecting to Oracle at {self.host}:{self.port} ({self.service_name})...")
        connect_args = {}
        if self.user.lower() == "sys":
            connect_args["mode"] = oracledb.AUTH_MODE_SYSDBA

        self.engine = create_engine(self.connection_uri, connect_args=connect_args)
        
        # Test connection
        with self.engine.connect() as conn:
            res = conn.execute(text("SELECT 1 FROM DUAL"))
            res.fetchone()
        logger.info("Oracle connection established.")

        self._ensure_schema()

    def _ensure_schema(self):
        """Creates target Oracle user/schema if it does not exist."""
        try:
            with self.engine.begin() as conn:
                try:
                    conn.execute(text('ALTER SESSION SET "_ORACLE_SCRIPT"=true'))
                except Exception:
                    pass
                
                check = conn.execute(
                    text("SELECT COUNT(*) FROM all_users WHERE username = :uname"),
                    {"uname": self.schema.upper()}
                )
                if check.fetchone()[0] == 0:
                    logger.info(f"Creating Oracle schema/user '{self.schema.upper()}'...")
                    conn.execute(text(f'CREATE USER {self.schema.upper()} IDENTIFIED BY "Sample_Data#123!"'))
                    conn.execute(text(f"GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO {self.schema.upper()}"))
                    logger.info(f"Schema '{self.schema.upper()}' created successfully.")
        except Exception as e:
            logger.warning(f"Schema check/creation note: {e}")

    def push_dataframe(self, df, table_name: str, mode: str = "append", partition_date: str = None, **kwargs):
        """
        Pushes a DataFrame (PySpark or Pandas) to target Oracle table.
        """
        sql_mode = "append" if mode == "append" else "replace"
        table_name_upper = table_name.upper()

        if hasattr(df, "toPandas"):
            try:
                pdf = df.toPandas()
            except Exception as e:
                logger.warning(f"Arrow toPandas conversion failed ({e}), falling back to collect()...")
                rows = [row.asDict() for row in df.collect()]
                pdf = pd.DataFrame(rows)
        else:
            pdf = df

        logger.info(f"Pushing DataFrame ({len(pdf)} rows) to Oracle table '{self.schema.upper()}.{table_name_upper}' (mode: {sql_mode})...")

        dtype_map = {}
        for col in pdf.columns:
            if pdf[col].dtype == 'float64':
                dtype_map[col] = Numeric(18, 2)
            elif pdf[col].dtype == 'int64' or pdf[col].dtype == 'int32':
                dtype_map[col] = Integer()
            elif pdf[col].dtype == 'object':
                dtype_map[col] = String(500)

        if sql_mode == "replace":
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f'DROP TABLE {self.schema.upper()}."{table_name_upper}" PURGE'))
            except Exception:
                pass

        pdf.to_sql(
            table_name_upper,
            con=self.engine,
            schema=self.schema.upper(),
            if_exists=sql_mode,
            index=False,
            chunksize=500,
            dtype=dtype_map,
        )

        logger.info(f"✅ Successfully written {len(pdf)} rows to {self.schema.upper()}.{table_name_upper}")

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("Oracle connection closed.")
