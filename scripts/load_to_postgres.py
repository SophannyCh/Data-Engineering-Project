# scripts/load_to_postgres.py

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres(df, table_name, db_url="postgresql://user:password@localhost:5432/retail"):
    print(f"[Load] Loading data into existing PostgreSQL table: {table_name}")

    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            # Load data into existing table using INSERT (no schema creation)
            df.to_sql(
                table_name,
                con=conn,
                if_exists="append",  # ⚠️ Only append to existing table
                index=False,
                method="multi"       # Efficient batch insert
            )
        print(f"✅ Data successfully loaded into '{table_name}'.")
    except Exception as e:
        print(f"❌ Error loading data into {table_name}: {e}")
