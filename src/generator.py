import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pyspark.sql.functions as F

from src.memory import Memory
from src.data_quality import DataQualityInjector

class SimulatorEngine:
    """
    The Brain of the Data Simulator (PySpark Version).
    Uses Spark for distributed generation, wrapping Faker and NumPy into Pandas iterators
    for infinite horizontal scalability on Databricks.
    """

    def __init__(self, spark: SparkSession, seed: int = 42, memory: Memory = None):
        self.spark = spark
        self.seed = seed
        self.memory = memory if memory else Memory()
        self.schema_blueprint = self._load_schema()

    def _load_schema(self, path="src/schema.json"):
        with open(path, "r") as f:
            return json.load(f)["tables"]

    def _get_spark_schema(self, table_schema) -> StructType:
        """Map generic JSON schema types to PySpark StructTypes"""
        fields = []
        for col in table_schema["columns"]:
            t = col["type"]
            if t == "number":
                if "semantic_tag" in col and "age" in col["semantic_tag"]:
                    stype = IntegerType()
                else:
                    stype = DoubleType()
            else:
                stype = StringType()
            fields.append(StructField(col["name"], stype, True))
        return StructType(fields)

    def _apply_business_rules(self, df, table_name):
        """
        Phase 3: Cross-Column Dependency Rules.
        Fixes logical anomalies like a 'Failed' order having a 'Delivery Date'.
        """
        if table_name == "restaurant_orders":
            if "order_status" in df.columns and "delivery_time" in df.columns:
                df = df.withColumn(
                    "delivery_time", 
                    F.when(F.col("order_status").isin("Failed", "Pending", "Cancelled"), F.lit(None))
                     .otherwise(F.col("delivery_time"))
                )
        
        # Add other cross-column logic here as needed
        return df

    def generate_table_batch(self, table_name: str, num_rows: int, start_time: datetime, time_increment_seconds: int = 60, anomaly_rate: float = 0.005):
        """
        Generates a Spark DataFrame of synthetic rows for a specific table.
        Uses `mapInPandas` for vectorized Python execution across Spark workers.
        """
        table_schema = next((t for t in self.schema_blueprint if t["table_name"] == table_name), None)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found in schema blueprint.")

        # Capture base offsets for the generators
        base_id = self.memory.get_next_id(table_name)
        # Base time for dates
        base_time_epoch = int(start_time.timestamp())

        # Extract instance variables into local scope to avoid pickling `self` (and SparkContext) 
        # when broadcasting `generate_partitions` to workers.
        seed_val = self.seed

        # --- Spark Worker Execution ---
        
        def generate_partitions(iterator):
            """
            This function runs distributed on Spark workers.
            It receives chunks of IDs and outputs realistic rows.
            """
            # Initialize generators once per Spark Task
            fake = Faker()
            
            for pdf_in in iterator:
                row_indices = pdf_in["_row_idx"].values
                n = len(row_indices)
                
                # NumPy vectorized random generations for this chunk
                np.random.seed(seed_val + int(row_indices[0]))
                Faker.seed(seed_val + int(row_indices[0]))
                
                # Prepare output dict
                out_data = {}
                
                # Pre-generate values column by column for performance
                for col in table_schema["columns"]:
                    col_name = col["name"]
                    tag = col.get("semantic_tag", "generic_string")
                    
                    if tag == "auto_increment_id":
                        out_data[col_name] = [base_id + idx for idx in row_indices]
                        
                    elif tag == "uuid":
                        # We use memory auto increment combined into a string to guarantee 
                        # determinism and no collisions across runs, better than pure random uuid4
                        out_data[col_name] = [f"{table_name}-{base_id + idx}" for idx in row_indices]
                        
                    elif tag == "sku_code":
                        out_data[col_name] = [f"SKU-{fake.lexify('????').upper()}{fake.numerify('####')}" for _ in range(n)]
                        
                    elif tag == "serial_code":
                        years = np.random.randint(2020, 2026, size=n)
                        out_data[col_name] = [f"SN-{y}-{fake.numerify('######')}" for y in years]
                        
                    elif tag == "terminal_code":
                        out_data[col_name] = [f"TERM-{fake.numerify('#####')}" for _ in range(n)]
                        
                    elif tag == "foreign_key_id":
                        # Generate independent reference strings since tables are decoupled
                        out_data[col_name] = [f"REF-{fake.lexify('??').upper()}{np.random.randint(1000, 9999)}" for _ in range(n)]
                        
                    elif tag == "date_of_birth":
                        out_data[col_name] = [fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat() for _ in range(n)]
                        
                    elif tag == "past_datetime" and ("time" in col_name.lower() or "date" in col_name.lower()):
                        # Exactly space out the increment
                        out_data[col_name] = [(datetime.fromtimestamp(base_time_epoch + (idx * time_increment_seconds))).strftime("%Y-%m-%d %H:%M:%S") for idx in row_indices]
                        
                    elif tag == "status_category":
                        out_data[col_name] = DataQualityInjector.generate_status_category(n)
                        
                    elif tag == "country_code":
                         out_data[col_name] = [fake.country_code() for _ in range(n)]
                         
                    elif tag == "gender":
                        out_data[col_name] = np.random.choice(["Male", "Female", "Non-Binary", "Prefer not to say"], p=[0.48, 0.48, 0.02, 0.02], size=n)
                        
                    elif tag == "financial_numeric":
                        out_data[col_name] = DataQualityInjector.generate_financial_numeric(n)
                        
                    elif tag == "age_numeric":
                         out_data[col_name] = DataQualityInjector.generate_age_numeric(n)
                         
                    elif tag == "rating_numeric":
                        out_data[col_name] = DataQualityInjector.generate_rating_numeric(n)
                        
                    elif tag == "email":
                        out_data[col_name] = [fake.company_email() for _ in range(n)]
                        
                    elif tag == "phone_number":
                        out_data[col_name] = [fake.phone_number() for _ in range(n)]
                        
                    elif tag == "first_name":
                        out_data[col_name] = [fake.first_name() for _ in range(n)]
                        
                    elif tag == "last_name":
                         out_data[col_name] = [fake.last_name() for _ in range(n)]
                         
                    elif tag == "person_name":
                        out_data[col_name] = [fake.name() for _ in range(n)]
                        
                    elif tag == "company_name":
                        out_data[col_name] = [fake.company() for _ in range(n)]
                        
                    else:
                        out_data[col_name] = [fake.word() for _ in range(n)]
                
                pdf_out = pd.DataFrame(out_data)
                
                # Anomaly injection
                pdf_out = DataQualityInjector.inject_anomalies(pdf_out, anomaly_rate)
                    
                yield pdf_out

        # Generate base dataframe of indices
        # If generating 100M rows, Spark handles this partition logic flawlessly
        df_range = self.spark.range(0, num_rows, 1, numPartitions=max(1, min(100, num_rows // 10000)))
        df_range = df_range.withColumnRenamed("id", "_row_idx")
        
        # Apply the mapping function
        spark_schema = self._get_spark_schema(table_schema)
        df_generated = df_range.mapInPandas(generate_partitions, spark_schema)
        
        # Apply strict cross-column business rules logic
        df_generated = self._apply_business_rules(df_generated, table_name)
        
        # Finally, update memory tracker (simulating evaluating the dataframe)
        # We do this logic manually because Spark is lazy.
        final_timestamp = datetime.fromtimestamp(base_time_epoch + ((num_rows - 1) * time_increment_seconds))
        
        self.memory.update_table_state(
            table_name=table_name,
            last_id=base_id + num_rows - 1,
            last_timestamp=final_timestamp.isoformat(),
            rows_added=num_rows
        )
        
        return df_generated
