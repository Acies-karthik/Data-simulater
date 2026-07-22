import os
import oracledb

admin_dir = r"C:\oracle\instantclient_21_15\network\admin"
os.makedirs(admin_dir, exist_ok=True)
sqlnet_path = os.path.join(admin_dir, "sqlnet.ora")

with open(sqlnet_path, "w") as f:
    f.write("SQLNET.AUTHENTICATION_SERVICES = (NONE)\nDISABLE_OOB = ON\n")

lib_dir = r"C:\oracle\instantclient_21_15"
try:
    oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=admin_dir)
except Exception as e:
    print("Client init note:", e)

dsn = oracledb.makedsn("92.4.85.120", 1521, service_name="saba_h3q_bom.preventualdbs2.vcn06021248.oraclevcn.com")
print("Connecting as SYSTEM to:", dsn)
conn = oracledb.connect(user="SYSTEM", password="Preventual_Acies#06#26", dsn=dsn)
cursor = conn.cursor()

print("Connected successfully!")
cursor.execute("SELECT username FROM all_users WHERE username = 'SAMPLE_DATASETS'")
rows = cursor.fetchall()
print("SAMPLE_DATASETS user count:", len(rows))

if len(rows) == 0:
    print("Creating user SAMPLE_DATASETS...")
    try:
        cursor.execute('ALTER SESSION SET "_ORACLE_SCRIPT"=true')
    except Exception as e:
        print("Alter session note:", e)
    cursor.execute('CREATE USER SAMPLE_DATASETS IDENTIFIED BY "Sample_Data#123!"')
    cursor.execute('GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO SAMPLE_DATASETS')
    print("User SAMPLE_DATASETS created and granted privileges.")
else:
    print("User SAMPLE_DATASETS already exists.")

cursor.close()
conn.close()
