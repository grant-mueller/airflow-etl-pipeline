import os
import pandas as pd
from sqlalchemy import create_engine

def load_to_db():
    df = pd.read_csv("/app/clean_data.csv")
    db_url = os.getenv("DATABASE_URL", "postgresql://user:pass@postgres:5432/mydb")
    engine = create_engine(db_url)
    df.to_sql("daily_snapshot", engine, if_exists='append', index=False)

if __name__ == "__main__":
    load_to_db()
