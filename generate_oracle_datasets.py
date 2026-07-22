"""
Oracle Data Generator — 100 Rows x 20 Columns Datasets
======================================================
Generates synthetic datasets with 100 rows and 20 columns each,
and pushes them directly into an Oracle database under the SAMPLE_DATASETS schema.

Usage:
    python generate_oracle_datasets.py
"""

import os
import sys
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.types import Numeric, Integer, String, DateTime
from urllib.parse import quote_plus
import oracledb

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Ensure Oracle Instant Client thick mode and SQLNET configuration (DISABLE_OOB=ON)
admin_dir = r"C:\oracle\instantclient_21_15\network\admin"
os.makedirs(admin_dir, exist_ok=True)
sqlnet_path = os.path.join(admin_dir, "sqlnet.ora")
with open(sqlnet_path, "w") as f:
    f.write("SQLNET.AUTHENTICATION_SERVICES = (NONE)\nDISABLE_OOB = ON\n")

lib_dir = r"C:\oracle\instantclient_21_15"
try:
    oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=admin_dir)
    logger.info(f"Oracle Instant Client initialized in thick mode from {lib_dir}.")
except oracledb.ProgrammingError as e:
    if "already" in str(e).lower():
        pass
    else:
        logger.error(f"Failed to init Oracle Client: {e}")

# Oracle Connection Configuration
CONFIG = {
    "user": "SYSTEM",
    "password": "Preventual_Acies#06#26",
    "host": "92.4.85.120",
    "port": 1521,
    "service_name": "saba_h3q_bom.preventualdbs2.vcn06021248.oraclevcn.com",
}

SCHEMA = "SAMPLE_DATASETS"
NUM_ROWS = 100

fake = Faker()
Faker.seed(42)
np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# Data Generators — Each produces 100 rows x 20 columns
# ─────────────────────────────────────────────────────────────────────────────

def generate_customer_profiles() -> pd.DataFrame:
    """CUSTOMER_PROFILES: 100 rows x 20 columns"""
    genders = ["Male", "Female", "Non-Binary", "Prefer Not to Say"]
    account_types = ["Basic", "Silver", "Gold", "Platinum", "Enterprise"]
    statuses = ["Active", "Active", "Active", "Pending", "Suspended"]
    
    first_names = [fake.first_name() for _ in range(NUM_ROWS)]
    last_names = [fake.last_name() for _ in range(NUM_ROWS)]
    
    return pd.DataFrame({
        "customer_id": range(1, NUM_ROWS + 1),
        "first_name": first_names,
        "last_name": last_names,
        "full_name": [f"{f} {l}" for f, l in zip(first_names, last_names)],
        "email": [fake.company_email() for _ in range(NUM_ROWS)],
        "phone_number": [fake.phone_number() for _ in range(NUM_ROWS)],
        "gender": list(np.random.choice(genders, size=NUM_ROWS)),
        "age": np.random.randint(18, 75, size=NUM_ROWS),
        "street_address": [fake.street_address() for _ in range(NUM_ROWS)],
        "city": [fake.city() for _ in range(NUM_ROWS)],
        "state": [fake.state_abbr() for _ in range(NUM_ROWS)],
        "postal_code": [fake.zipcode() for _ in range(NUM_ROWS)],
        "country": [fake.country_code() for _ in range(NUM_ROWS)],
        "account_type": list(np.random.choice(account_types, size=NUM_ROWS)),
        "account_status": list(np.random.choice(statuses, size=NUM_ROWS)),
        "credit_score": np.random.randint(580, 850, size=NUM_ROWS),
        "annual_income": np.round(np.random.normal(loc=85000, scale=30000, size=NUM_ROWS), 2),
        "total_purchases": np.random.randint(1, 150, size=NUM_ROWS),
        "last_login_date": [
            (datetime(2025, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d %H:%M:%S")
            for d in np.random.randint(0, 500, size=NUM_ROWS)
        ],
        "signup_date": [
            (datetime(2022, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in np.random.randint(0, 1000, size=NUM_ROWS)
        ],
    })


def generate_sales_transactions() -> pd.DataFrame:
    """SALES_TRANSACTIONS: 100 rows x 20 columns"""
    channels = ["Web", "Mobile App", "In-Store", "Partner API", "Phone"]
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Beauty", "Sports"]
    methods = ["Credit Card", "Debit Card", "PayPal", "Wire Transfer", "Apple Pay"]
    statuses = ["Completed", "Completed", "Completed", "Pending", "Refunded"]
    shipping_methods = ["Standard", "Express", "Overnight", "Same-Day", "Curbside"]
    
    qty = np.random.randint(1, 10, size=NUM_ROWS)
    unit_price = np.round(np.random.uniform(10.0, 750.0, size=NUM_ROWS), 2)
    gross = np.round(qty * unit_price, 2)
    discount_pct = np.round(np.random.choice([0.0, 0.05, 0.10, 0.15, 0.20], size=NUM_ROWS), 2)
    net = np.round(gross * (1.0 - discount_pct), 2)
    tax = np.round(net * 0.08, 2)
    total = np.round(net + tax, 2)
    
    return pd.DataFrame({
        "transaction_id": range(1001, 1001 + NUM_ROWS),
        "customer_id": np.random.randint(1, 101, size=NUM_ROWS),
        "product_id": np.random.randint(2001, 2500, size=NUM_ROWS),
        "transaction_timestamp": [
            (datetime(2025, 1, 1) + timedelta(minutes=int(m))).strftime("%Y-%m-%d %H:%M:%S")
            for m in np.cumsum(np.random.randint(10, 180, size=NUM_ROWS))
        ],
        "store_id": np.random.randint(10, 50, size=NUM_ROWS),
        "channel": list(np.random.choice(channels, size=NUM_ROWS)),
        "product_category": list(np.random.choice(categories, size=NUM_ROWS)),
        "quantity": qty,
        "unit_price": unit_price,
        "gross_amount": gross,
        "discount_percent": discount_pct,
        "net_amount": net,
        "tax_amount": tax,
        "total_amount": total,
        "payment_method": list(np.random.choice(methods, size=NUM_ROWS)),
        "payment_status": list(np.random.choice(statuses, size=NUM_ROWS)),
        "currency": ["USD"] * NUM_ROWS,
        "shipping_method": list(np.random.choice(shipping_methods, size=NUM_ROWS)),
        "shipping_cost": np.round(np.random.uniform(0.0, 25.0, size=NUM_ROWS), 2),
        "is_flagged_fraud": np.random.choice([0, 0, 0, 0, 1], size=NUM_ROWS),
    })


def generate_product_inventory() -> pd.DataFrame:
    """PRODUCT_INVENTORY: 100 rows x 20 columns"""
    mains = ["Electronics", "Fashion", "Home Improvement", "Health & Wellness", "Toys & Games"]
    subs = ["Smartphones", "Laptops", "Casual Wear", "Power Tools", "Vitamins", "Board Games"]
    brands = ["NexusTech", "AeroStyle", "Vanguard", "EchoLine", "ApexGear", "OmniCraft"]
    locations = ["WH-East", "WH-West", "WH-North", "WH-South", "WH-Central"]
    
    cost = np.round(np.random.uniform(5.0, 300.0, size=NUM_ROWS), 2)
    retail = np.round(cost * np.random.uniform(1.3, 2.2, size=NUM_ROWS), 2)
    wholesale = np.round(cost * np.random.uniform(1.1, 1.4, size=NUM_ROWS), 2)
    
    return pd.DataFrame({
        "product_id": range(2001, 2001 + NUM_ROWS),
        "sku_code": [f"SKU-{fake.bothify(text='??-####').upper()}" for _ in range(NUM_ROWS)],
        "product_name": [f"{fake.word().capitalize()} {fake.word().capitalize()}" for _ in range(NUM_ROWS)],
        "brand_name": list(np.random.choice(brands, size=NUM_ROWS)),
        "main_category": list(np.random.choice(mains, size=NUM_ROWS)),
        "sub_category": list(np.random.choice(subs, size=NUM_ROWS)),
        "supplier_id": np.random.randint(501, 550, size=NUM_ROWS),
        "supplier_name": [fake.company() for _ in range(NUM_ROWS)],
        "unit_cost": cost,
        "retail_price": retail,
        "wholesale_price": wholesale,
        "stock_quantity": np.random.randint(0, 1500, size=NUM_ROWS),
        "reorder_level": np.random.randint(50, 200, size=NUM_ROWS),
        "reorder_quantity": np.random.randint(100, 500, size=NUM_ROWS),
        "warehouse_location": list(np.random.choice(locations, size=NUM_ROWS)),
        "aisle_number": [f"Aisle-{np.random.randint(1, 40):02d}" for _ in range(NUM_ROWS)],
        "is_active": np.random.choice([1, 1, 1, 0], size=NUM_ROWS),
        "rating_score": np.round(np.random.uniform(3.0, 5.0, size=NUM_ROWS), 1),
        "last_restock_date": [
            (datetime(2025, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in np.random.randint(0, 400, size=NUM_ROWS)
        ],
        "created_date": [
            (datetime(2023, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in np.random.randint(0, 700, size=NUM_ROWS)
        ],
    })


def generate_employee_records() -> pd.DataFrame:
    """EMPLOYEE_RECORDS: 100 rows x 20 columns"""
    jobs = ["Software Engineer", "Data Scientist", "Sales Manager", "Financial Analyst", "HR Specialist", "Product Manager"]
    depts = ["Engineering", "Sales", "Marketing", "Finance", "Human Resources", "Operations"]
    divisions = ["North America", "EMEA", "APAC", "LATAM", "Global Corporate"]
    types = ["Full-Time", "Full-Time", "Full-Time", "Contract", "Part-Time"]
    locations = ["New York", "San Francisco", "London", "Tokyo", "Remote"]
    
    firsts = [fake.first_name() for _ in range(NUM_ROWS)]
    lasts = [fake.last_name() for _ in range(NUM_ROWS)]
    salary = np.round(np.random.normal(loc=90000, scale=25000, size=NUM_ROWS), 2)
    bonus_pct = np.round(np.random.uniform(0.05, 0.25, size=NUM_ROWS), 2)
    tot_comp = np.round(salary * (1.0 + bonus_pct), 2)
    
    return pd.DataFrame({
        "employee_id": range(5001, 5001 + NUM_ROWS),
        "first_name": firsts,
        "last_name": lasts,
        "job_title": list(np.random.choice(jobs, size=NUM_ROWS)),
        "department": list(np.random.choice(depts, size=NUM_ROWS)),
        "division": list(np.random.choice(divisions, size=NUM_ROWS)),
        "manager_id": np.random.randint(5001, 5020, size=NUM_ROWS),
        "email": [f"{f.lower()}.{l.lower()}@company.com" for f, l in zip(firsts, lasts)],
        "phone_extension": [f"x{np.random.randint(1000, 9999)}" for _ in range(NUM_ROWS)],
        "work_location": list(np.random.choice(locations, size=NUM_ROWS)),
        "employment_type": list(np.random.choice(types, size=NUM_ROWS)),
        "hire_date": [
            (datetime(2017, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in np.random.randint(0, 2800, size=NUM_ROWS)
        ],
        "base_salary": salary,
        "bonus_pct": bonus_pct,
        "total_compensation": tot_comp,
        "performance_rating": np.random.randint(1, 6, size=NUM_ROWS),
        "projects_completed": np.random.randint(2, 40, size=NUM_ROWS),
        "vacation_days_left": np.random.randint(0, 25, size=NUM_ROWS),
        "last_review_date": [
            (datetime(2025, 1, 1) + timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in np.random.randint(0, 300, size=NUM_ROWS)
        ],
        "is_remote": np.random.choice([1, 0], size=NUM_ROWS),
    })


def generate_server_metrics() -> pd.DataFrame:
    """SERVER_METRICS: 100 rows x 20 columns"""
    zones = ["us-east-1a", "us-east-1b", "us-west-2a", "eu-west-1a", "ap-northeast-1a"]
    envs = ["Production", "Production", "Staging", "Development"]
    statuses = ["HEALTHY", "HEALTHY", "HEALTHY", "WARNING", "CRITICAL"]
    
    cpu = np.round(np.random.uniform(5.0, 99.0, size=NUM_ROWS), 1)
    mem_total = 32768
    mem_used = np.random.randint(4096, 30000, size=NUM_ROWS)
    disk_pct = np.round(np.random.uniform(20.0, 95.0, size=NUM_ROWS), 1)
    disk_free_gb = np.round((100.0 - disk_pct) * 5.0, 1)
    
    return pd.DataFrame({
        "metric_id": range(10001, 10001 + NUM_ROWS),
        "recorded_at": [
            (datetime(2025, 6, 1) + timedelta(seconds=int(s))).strftime("%Y-%m-%d %H:%M:%S")
            for s in np.cumsum(np.random.randint(60, 600, size=NUM_ROWS))
        ],
        "server_hostname": [f"srv-prod-{np.random.randint(1, 20):02d}.internal" for _ in range(NUM_ROWS)],
        "server_ip": [f"10.0.{np.random.randint(1, 10)}.{np.random.randint(2, 250)}" for _ in range(NUM_ROWS)],
        "datacenter_zone": list(np.random.choice(zones, size=NUM_ROWS)),
        "environment": list(np.random.choice(envs, size=NUM_ROWS)),
        "cpu_usage_pct": cpu,
        "memory_usage_mb": mem_used,
        "memory_total_mb": [mem_total] * NUM_ROWS,
        "disk_usage_pct": disk_pct,
        "disk_free_gb": disk_free_gb,
        "network_in_mbps": np.round(np.random.uniform(10.0, 950.0, size=NUM_ROWS), 2),
        "network_out_mbps": np.round(np.random.uniform(20.0, 1200.0, size=NUM_ROWS), 2),
        "active_connections": np.random.randint(50, 5000, size=NUM_ROWS),
        "request_count": np.random.randint(1000, 100000, size=NUM_ROWS),
        "response_time_ms": np.round(np.random.lognormal(mean=3.5, sigma=0.6, size=NUM_ROWS), 1),
        "error_count": np.random.randint(0, 50, size=NUM_ROWS),
        "http_5xx_rate": np.round(np.random.uniform(0.0, 2.5, size=NUM_ROWS), 3),
        "uptime_seconds": np.random.randint(86400, 5000000, size=NUM_ROWS),
        "health_status": list(np.random.choice(statuses, size=NUM_ROWS)),
    })


DATASETS = {
    "CUSTOMER_PROFILES": generate_customer_profiles,
    "SALES_TRANSACTIONS": generate_sales_transactions,
    "PRODUCT_INVENTORY": generate_product_inventory,
    "EMPLOYEE_RECORDS": generate_employee_records,
    "SERVER_METRICS": generate_server_metrics,
}


def build_oracle_engine(config: dict):
    pwd_escaped = quote_plus(config["password"])
    user = config["user"]
    uri = (
        f"oracle+oracledb://{user}:{pwd_escaped}@{config['host']}:{config['port']}"
        f"/?service_name={config['service_name']}"
    )
    return create_engine(uri)


def main():
    logger.info("=" * 70)
    logger.info("Oracle Data Generator — 5 Datasets x 100 Rows x 20 Columns")
    logger.info("Target Schema: SAMPLE_DATASETS")
    logger.info("=" * 70)

    engine = build_oracle_engine(CONFIG)

    # Test Oracle connection
    with engine.connect() as conn:
        res = conn.execute(text("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual"))
        logger.info(f"Connected to Oracle. Current Schema: {res.fetchone()[0]}")

    results_summary = []

    for table_name, generator_func in DATASETS.items():
        logger.info("-" * 50)
        logger.info(f"Generating dataset: {table_name}")
        df = generator_func()

        logger.info(f"  Dimensions: {df.shape[0]} rows x {df.shape[1]} columns")
        
        dtype_map = {}
        for col in df.columns:
            if df[col].dtype == 'float64':
                dtype_map[col] = Numeric(18, 2)
            elif df[col].dtype == 'int64' or df[col].dtype == 'int32':
                dtype_map[col] = Integer()
            elif df[col].dtype == 'object':
                dtype_map[col] = String(500)

        # Drop existing table if exists
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP TABLE {SCHEMA}."{table_name}" PURGE'))
        except Exception:
            pass

        # Insert table into SAMPLE_DATASETS schema
        try:
            df.to_sql(
                table_name.upper(),
                con=engine,
                schema=SCHEMA.upper(),
                if_exists="replace",
                index=False,
                chunksize=100,
                dtype=dtype_map,
            )
            status = "SUCCESS"
            logger.info(f"  [OK] Successfully pushed 100 rows x 20 columns to {SCHEMA}.{table_name}")
        except Exception as e:
            status = f"FAILED: {e}"
            logger.error(f"  [FAIL] Failed writing to {SCHEMA}.{table_name}: {e}")

        results_summary.append({
            "table_name": table_name,
            "rows": df.shape[0],
            "columns": df.shape[1],
            "schema": SCHEMA,
            "status": status,
        })

    logger.info("=" * 70)
    logger.info("GENERATION & DATA LOAD COMPLETE")
    logger.info("=" * 70)
    summary_df = pd.DataFrame(results_summary)
    print("\n" + summary_df.to_string(index=False) + "\n")
    engine.dispose()


if __name__ == "__main__":
    main()
