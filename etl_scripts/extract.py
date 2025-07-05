import requests
import pandas as pd

def extract_data():
    base_url = "https://api.example.com/data"
    page = 1
    data = []

    while True:
        resp = requests.get(f"{base_url}?page={page}")
        if resp.status_code != 200:
            break
        results = resp.json().get("results", [])
        if not results:
            break
        data.extend(results)
        page += 1

    df = pd.DataFrame(data)
    df.to_csv("/app/raw_data.csv", index=False)

if __name__ == "__main__":
    extract_data()
