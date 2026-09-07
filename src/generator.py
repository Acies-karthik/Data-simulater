import json
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F

from src.memory import Memory
from src.data_quality import DataQualityInjector, load_rules_config

class SimulatorEngine:
    """
    The Brain of the Data Simulator.
    Generates data in pandas on the driver then converts to a Spark DataFrame.
    This avoids PySpark 4.x's mandatory Arrow serialization in mapInPandas,
    which cannot handle NaN in typed numeric columns.
    """

    def __init__(self, spark: SparkSession, seed: int = 42, memory: Memory = None, schema_path: str = "src/schema.json"):
        self.spark = spark
        self.seed = seed
        self.memory = memory if memory else Memory()
        self.schema_blueprint = self._load_schema(schema_path)
        # Load rules config once at engine startup
        rules_path = os.environ.get("RULES_CONFIG_PATH", "rules_config.json")
        self.rules = load_rules_config(rules_path)
        self.dq_injector = DataQualityInjector(rules=self.rules)

    def _load_schema(self, path="src/schema.json"):
        with open(path, "r") as f:
            return json.load(f)["tables"]

    def _get_spark_schema(self, table_schema) -> StructType:
        """Map generic JSON schema types to PySpark StructTypes.
        All numeric columns use DoubleType (float64) so NaN values from
        null injection are Arrow-compatible. Integer types cannot hold NaN."""
        fields = []
        for col in table_schema["columns"]:
            t = col["type"]
            if t == "number":
                stype = DoubleType()  # always float so NaN injection works with Arrow
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
        return df

    def generate_table_batch(self, table_name: str, num_rows: int, start_time: datetime, time_increment_seconds: int = 60, anomaly_rate: float = 0.0):
        """
        Generates a Spark DataFrame of synthetic rows for a specific table.
        Generates all data in pandas on the driver, applies anomaly injection,
        then converts to a Spark DataFrame using an explicit schema.
        This avoids the mandatory Arrow serialization in PySpark 4.x mapInPandas.
        """
        table_schema = next((t for t in self.schema_blueprint if t["table_name"] == table_name), None)
        if not table_schema:
            raise ValueError(f"Table {table_name} not found in schema blueprint.")

        base_id = self.memory.get_next_id(table_name)
        base_time_epoch = int(start_time.timestamp())

        fake = Faker()
        np.random.seed(self.seed)
        Faker.seed(self.seed)

        row_indices = np.arange(num_rows)
        n = num_rows
        out_data = {}

        for col in table_schema["columns"]:
            col_name = col["name"]
            tag = col.get("semantic_tag", "generic_string")

            if tag == "auto_increment_id":
                out_data[col_name] = [str(base_id + idx) for idx in row_indices]
            elif tag == "uuid":
                out_data[col_name] = [f"{table_name}-{base_id + idx}" for idx in row_indices]
            elif tag == "sku_code":
                out_data[col_name] = [f"SKU-{fake.lexify('????').upper()}{fake.numerify('####')}" for _ in range(n)]
            elif tag == "serial_code":
                years = np.random.randint(2020, 2026, size=n)
                out_data[col_name] = [f"SN-{y}-{fake.numerify('######')}" for y in years]
            elif tag == "terminal_code":
                out_data[col_name] = [f"TERM-{fake.numerify('#####')}" for _ in range(n)]
            elif tag == "foreign_key_id":
                out_data[col_name] = [f"REF-{fake.lexify('??').upper()}{np.random.randint(1000, 9999)}" for _ in range(n)]
            elif tag == "date_of_birth":
                out_data[col_name] = [fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat() for _ in range(n)]
            elif tag == "past_datetime" and ("time" in col_name.lower() or "date" in col_name.lower()):
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
            elif tag == "city":
                out_data[col_name] = [fake.city() for _ in range(n)]
            else:
                out_data[col_name] = [fake.word() for _ in range(n)]

        pdf = pd.DataFrame(out_data)

        # Anomaly injection (rules-driven) — runs on pandas before Spark conversion
        pdf = self.dq_injector.inject_anomalies(pdf, anomaly_rate)

        # Cast all numeric columns explicitly to float64 so Spark/Arrow handles NaN correctly
        spark_schema = self._get_spark_schema(table_schema)
        for field in spark_schema.fields:
            if isinstance(field.dataType, DoubleType) and field.name in pdf.columns:
                pdf[field.name] = pd.to_numeric(pdf[field.name], errors="coerce").astype("float64")

        # Convert pandas → Spark with explicit schema (avoids Arrow type mismatch)
        df_generated = self.spark.createDataFrame(pdf, schema=spark_schema)

        # Apply cross-column business rules
        df_generated = self._apply_business_rules(df_generated, table_name)

        # Update memory tracker
        final_timestamp = datetime.fromtimestamp(base_time_epoch + ((num_rows - 1) * time_increment_seconds))
        self.memory.update_table_state(
            table_name=table_name,
            last_id=base_id + num_rows - 1,
            last_timestamp=final_timestamp.isoformat(),
            rows_added=num_rows
        )

        return df_generated
