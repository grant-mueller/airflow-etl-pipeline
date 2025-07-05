import os
import pandas as pd
from sqlalchemy import create_engine, text

def run_analytics():
    db_url = os.getenv("DATABASE_URL", "postgresql://user:pass@postgres:5432/mydb")
    engine = create_engine(db_url)
    query = text("""
        SELECT user_id, SUM(amount) AS total_spent
        FROM daily_snapshot
        WHERE created_at >= current_date - interval '7 days'
        GROUP BY user_id
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=['user_id','total_spent'])
        df.to_csv("/app/weekly_summary.csv", index=False)

if __name__ == "__main__":
    run_analytics()
