import pandas as pd

def clean_data():
    df = pd.read_csv("/app/raw_data.csv")
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['amount'] = df['amount'].fillna(0).astype(float)
    df = df[df['status'] == 'active']
    df.to_csv("/app/clean_data.csv", index=False)

if __name__ == "__main__":
    clean_data()
