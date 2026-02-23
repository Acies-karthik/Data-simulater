import argparse
from datetime import datetime
import json
import logging
import time
import sys
import os
from dotenv import load_dotenv

# Databricks Pure-Python Environment Override
try:
    # Databricks exec() environment drops __file__, so we use os.getcwd()
    import sys
    script_dir = os.getcwd()
    if script_dir not in sys.path:
        sys.path.append(script_dir)
        
    from databricks_env import POSTGRES_URI
    if POSTGRES_URI:
        os.environ["POSTGRES_URI"] = POSTGRES_URI
        print("Successfully loaded POSTGRES_URI from databricks_env.py")
except ImportError:
    print("Warning: databricks_env.py not found. Falling back to default os.environ logic.")

# Fix for Windows PySpark worker 'Python not found' errors
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Fix for Windows Hadoop pathing (forces PySpark to look in C:\hadoop\bin)
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from pyspark.sql import SparkSession

from src.generator import SimulatorEngine
from src.memory import Memory
from src.connectors.file_connector import FileConnector
from src.connectors.postgres_connector import PostgresConnector
from src.connectors.snowflake_connector import SnowflakeConnector

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_connector(target: str, output_dir: str = "output_data", s3_bucket: str = None):
    """Instantiate the correct connector based on CLI flag."""
    if target == "postgres":
        return PostgresConnector()
    elif target == "snowflake":
        return SnowflakeConnector()
    elif target in ["file", "csv", "parquet"]:
        fmt = "csv" if target == "csv" else "parquet"
        
        # If S3 bucket provided, override the output directory to an s3a:// path
        if s3_bucket:
             output_dir = f"s3a://{s3_bucket}/{output_dir}"
        
        return FileConnector(format=fmt, output_dir=output_dir)
    else:
        raise ValueError(f"Unknown target: {target}")

def get_tables(schema_path="src/schema.json"):
    """
    Parses the schema blueprint and returns all tables.
    Datasets are generated independently to maximize horizontal scalability.
    """
    with open(schema_path, "r") as f:
        schema = json.load(f)["tables"]
    return [t["table_name"] for t in schema]

def init_spark():
    """Initialize a local PySpark session if running standalone. 
    In Databricks, spark is automatically injected."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder \
            .appName("PreventualDataSimulator") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
    except Exception as e:
        logger.error(f"Failed to initialize PySpark: {str(e)}\nPlease make sure PySpark is correctly installed.")
        raise

def run_phase_a(spark: SparkSession, target: str, initial_rows: int = 1000, specific_table: str = None):
    """
    Phase A: The Big Bang.
    """
    logger.info("Starting Phase A (Initial Load - The Big Bang) via PySpark Distributed Engine")
    
    memory = Memory()
    memory.clear()
    
    engine = SimulatorEngine(spark=spark, memory=memory)
    connector = get_connector(target)
    
    start_time = datetime(2025, 1, 1, 0, 0, 0)
    
    tables = get_tables()
    if specific_table:
        if specific_table not in tables:
            raise ValueError(f"Table '{specific_table}' not found in schema blueprint.")
        tables = [specific_table]
        
    logger.info(f"Loaded {len(tables)} tables from blueprint. Generating independently.")
    
    connector.connect()
    try:
        for table_name in tables:
            logger.info(f"Generating {initial_rows} rows for {table_name}...")
            
            # Massive horizontal scale generation via PySpark mapInPandas
            df = engine.generate_table_batch(
                table_name=table_name,
                num_rows=initial_rows,
                start_time=start_time,
                time_increment_seconds=600
            )
            
            # Persist to target using PySpark native tools (Partitioned by initial load date)
            partition_date = start_time.strftime("%Y-%m-%d")
            connector.push_dataframe(df, table_name, mode="replace", partition_date=partition_date)
                     
    finally:
        connector.close()
        memory.save()
        
    logger.info("Phase A Complete. State saved in catalog.json.")


def run_phase_b(spark: SparkSession, target: str, incremental_rows: int = 50):
    """
    Phase B: The Heartbeat.
    Reads current state from memory and generates new incremental rows via PySpark.
    """
    logger.info("Starting Phase B (Incremental Load - The Heartbeat) via PySpark")
    
    memory = Memory()
    if not memory.state.get("last_run"):
        logger.error("Memory state is empty! You must run Phase A (init) before Phase B.")
        return
        
    engine = SimulatorEngine(spark=spark, memory=memory)
    connector = get_connector(target)
    
    tables = get_tables()
    
    connector.connect()
    try:
        for table_name in tables:
            table_state = memory.get_table_state(table_name)
            last_timestamp_str = table_state.get("last_timestamp")
            
            if last_timestamp_str:
                start_time = datetime.fromisoformat(last_timestamp_str)
            else:
                 start_time = datetime.now()
                 
            logger.info(f"Simulating {incremental_rows} new events for {table_name} starting exactly after {start_time}")
            
            df = engine.generate_table_batch(
                table_name=table_name,
                num_rows=incremental_rows,
                start_time=start_time,
                time_increment_seconds=30
            )
            
            # Push incrementally to partitioned data lake folder or append to db
            partition_date = start_time.strftime("%Y-%m-%d")
            connector.push_dataframe(df, table_name, mode="append", partition_date=partition_date)
            
    finally:
        connector.close()
        memory.save()
        
    logger.info("Phase B Complete. State advanced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enterprise Synthetic PySpark Data Simulator")
    parser.add_argument("--mode", choices=["init", "stream"], required=True, 
                        help="'init' for Phase A (Big Bang) or 'stream' for Phase B (Incremental).")
    parser.add_argument("--target", choices=["file", "parquet", "csv", "postgres", "snowflake"], default="file",
                        help="Target destination for output data.")
    parser.add_argument("--rows", type=int, default=None,
                        help="Number of rows per table. Default: 1000 for init, 50 for stream.")
    parser.add_argument("--output-dir", type=str, default="output_data",
                        help="Directory for file outputs. Default: output_data/")
    parser.add_argument("--s3-bucket", type=str, default=None,
                        help="Optional S3 bucket name (e.g., 'my-data-lake-bucket'). If provided, overrides output-dir to use s3a://")
    parser.add_argument("--table", type=str, default=None,
                        help="Specify a single table to generate (e.g., users). Default is all tables.")
                        
    args = parser.parse_args()
    
    logger.info("Initializing PySpark Execution Context...")
    spark_session = init_spark()
    
    # Global connector instantiation
    global_connector = get_connector(args.target, output_dir=args.output_dir, s3_bucket=args.s3_bucket)
    
    if args.mode == "init":
        rows = args.rows if args.rows is not None else 1000
        run_phase_a(spark=spark_session, target=args.target, initial_rows=rows, specific_table=args.table)
    elif args.mode == "stream":
        rows = args.rows if args.rows is not None else 50
        run_phase_b(spark=spark_session, target=args.target, incremental_rows=rows)
