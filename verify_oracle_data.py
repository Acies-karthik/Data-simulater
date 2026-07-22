import os
import oracledb

admin_dir = r"C:\oracle\instantclient_21_15\network\admin"
lib_dir = r"C:\oracle\instantclient_21_15"
try:
    oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=admin_dir)
except Exception:
    pass

dsn = oracledb.makedsn("92.4.85.120", 1521, service_name="saba_h3q_bom.preventualdbs2.vcn06021248.oraclevcn.com")
conn = oracledb.connect(user="SYSTEM", password="Preventual_Acies#06#26", dsn=dsn)
cursor = conn.cursor()

tables = ["CUSTOMER_PROFILES", "SALES_TRANSACTIONS", "PRODUCT_INVENTORY", "EMPLOYEE_RECORDS", "SERVER_METRICS"]

print("=" * 80)
print("ORACLE VERIFICATION: SAMPLE_DATASETS SCHEMA")
print("=" * 80)

for tbl in tables:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM SAMPLE_DATASETS."{tbl}"')
        row_count = cursor.fetchone()[0]
        cursor.execute(f'SELECT * FROM SAMPLE_DATASETS."{tbl}" WHERE ROWNUM = 1')
        cols = [col[0] for col in cursor.description]
        print(f"Table: SAMPLE_DATASETS.{tbl}")
        print(f"  Rows    : {row_count}")
        print(f"  Columns : {len(cols)}")
        print(f"  Column List : {cols}\n")
    except Exception as e:
        print(f"Table: SAMPLE_DATASETS.{tbl} Error: {e}\n")

cursor.close()
conn.close()
