import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()
uri = os.environ.get("POSTGRES_URI")

if not uri:
    print("❌ ERROR: POSTGRES_URI is not set in the .env file.")
    exit(1)

print(f"Testing connection to: {uri.split('@')[-1]}") # Print safely without password

try:
    # Connect directly to the Cloud DB URI
    engine = create_engine(uri)
    
    # Create the schema
    with engine.begin() as con:
        con.exec_driver_sql('CREATE SCHEMA IF NOT EXISTS "Sample_datasets";')
        
    print("✅ Connected to Cloud Postgres and verified 'Sample_datasets' schema!")
    
    # Insert test data to prove write capabilities
    df = pd.DataFrame({'test_id': [1, 2, 3], 'status': ['Cloud', 'Cloud', 'Cloud']})
    df.to_sql('connection_test', con=engine, schema='Sample_datasets', if_exists='replace', index=False)
    
    print("✅ Successfully inserted test tabular data into Cloud Sample_datasets.connection_test!")
except Exception as e:
    print(f"❌ Error testing SQLAlchemy: {e}")
