import os
import oracledb

lib_dir = r"C:\oracle\instantclient_21_15"
admin_dir = r"C:\oracle\instantclient_21_15\network\admin"

oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=admin_dir)

dsn = oracledb.makedsn("92.4.85.120", 1521, service_name="saba_h3q_bom.preventualdbs2.vcn06021248.oraclevcn.com")
conn = oracledb.connect(user="SYSTEM", password="Preventual_Acies#06#26", dsn=dsn)
cur = conn.cursor()

print("=" * 70)
print("VERIFYING SAMPLE_DATASETS SCHEMA IN ORACLE DB")
print("=" * 70)

cur.execute("SELECT table_name FROM all_tables WHERE owner = 'SAMPLE_DATASETS' ORDER BY table_name")
tables = [row[0] for row in cur.fetchall()]

for table in tables:
    cur.execute(f'SELECT COUNT(*) FROM SAMPLE_DATASETS."{table}"')
    row_count = cur.fetchone()[0]
    
    cur.execute(f'SELECT column_name FROM all_tab_columns WHERE owner = \'SAMPLE_DATASETS\' AND table_name = \'{table}\'')
    cols = cur.fetchall()
    col_count = len(cols)
    
    print(f"Table: SAMPLE_DATASETS.{table:<22} | Rows: {row_count:<5} | Columns: {col_count}")

cur.close()
conn.close()
print("=" * 70)
