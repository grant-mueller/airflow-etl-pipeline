# %%
# [markdown]
# # 📓 Interactive Data Pipeline Demo
#
# This notebook demonstrates a **scalable**, **modular** data pipeline in Python:
#
# 1. Extraction (CSV / API)  
# 2. Transformation (Pandas)  
# 3. Loading (SQLite via SQLAlchemy)  
# 4. Orchestration (button-driven)  
# 5. Observability (logging & progress bars)  
# 6. Testing (in-notebook assertions)
#
# Use the widgets below to tweak parameters and run the pipeline end-to-end!

# %%
# Install required packages (uncomment if running first time)
# %pip install pandas sqlalchemy ipywidgets tqdm plotly

# %%
# [markdown]
# ## 📦 Imports & Configuration

# %%
import os
import sqlite3
import logging
from tqdm.auto import tqdm
from IPython.display import display, Markdown
import pandas as pd
import sqlalchemy as sa
import ipywidgets as widgets
import plotly.express as px

# %%
# Configuration
CONFIG = {
    "data_sources": {
        "Local CSV": "./clinical_trials_sample.csv",
        # add more sources or API endpoints here
    },
    "db_url": "sqlite:///pipeline_demo.db",
    "default_chunk": 5000
}

# ensure sample CSV exists (or generate dummy)
if not os.path.exists(CONFIG["data_sources"]["Local CSV"]):
    df_dummy = pd.DataFrame({
        "patient_id": range(1, 10001),
        "visit_date": pd.date_range("2025-01-01", periods=10000, freq="H"),
        "heart_rate": (60 + 20 * pd.np.sin(pd.np.linspace(0, 50, 10000))).astype(int),
        "lab_value": pd.np.random.normal(100, 15, size=10000).round(2)
    })
    df_dummy.to_csv(CONFIG["data_sources"]["Local CSV"], index=False)

# set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("pipeline")

# %%
# [markdown]
# ## 🔍 1. Extraction

# %%
def extract_data(source: str, chunksize: int):
    """
    Yields DataFrame chunks from the selected source.
    """
    path = CONFIG["data_sources"][source]
    logger.info(f"Starting extract from {path}")
    for chunk in pd.read_csv(path, chunksize=chunksize, parse_dates=["visit_date"]):
        yield chunk
    logger.info("Extraction complete")

# %%
# Extraction Widget
source_w = widgets.Dropdown(
    options=list(CONFIG["data_sources"].keys()),
    description="Source:",
)
chunk_w = widgets.IntSlider(
    value=CONFIG["default_chunk"], min=1000, max=10000, step=1000,
    description="Chunk Size:",
)
display(source_w, chunk_w)

# %%
# [markdown]
# ## 🛠️ 2. Transformation

# %%
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies basic transformations:
    - compute BPM category
    - normalize lab_value
    """
    logger.info(f"Transforming chunk of size {len(df)}")
    df = df.copy()
    df["bpm_category"] = pd.cut(
        df["heart_rate"],
        bins=[0, 60, 100, 200],
        labels=["low", "normal", "high"]
    )
    df["lab_value_norm"] = (
        (df["lab_value"] - df["lab_value"].mean()) / df["lab_value"].std()
    )
    return df

# %%
# [markdown]
# ## 💾 3. Loading

# %%
def load_data(df: pd.DataFrame, table: str = "clinical_data"):
    """
    Loads a DataFrame into the configured database.
    """
    engine = sa.create_engine(CONFIG["db_url"], echo=False, pool_pre_ping=True)
    with engine.begin() as conn:
        df.to_sql(table, conn, if_exists="append", index=False)
    logger.info(f"Loaded {len(df)} rows into `{table}`")

# %%
# [markdown]
# ## ▶️ 4. Orchestration

# %%
def run_pipeline(source: str, chunksize: int):
    """
    Runs extract → transform → load end-to-end, showing a progress bar.
    """
    engine = sa.create_engine(CONFIG["db_url"])
    # drop table to start fresh
    with engine.begin() as conn:
        conn.execute("DROP TABLE IF EXISTS clinical_data;")
    total_rows = 0
    chunks = list(pd.read_csv(CONFIG["data_sources"][source], chunksize=chunksize))
    pbar = tqdm(chunks, desc="Pipeline chunks", unit="chunk")
    for chunk in pbar:
        transformed = transform_data(chunk)
        load_data(transformed)
        total_rows += len(transformed)
        pbar.set_postfix(rows=total_rows)
    display(Markdown(f"**✅ Pipeline complete. Total rows processed: {total_rows:,}**"))

# %%
# Run button
run_button = widgets.Button(description="Run Full Pipeline", button_style="success")
output_area = widgets.Output()

def on_run_clicked(b):
    with output_area:
        output_area.clear_output()
        run_pipeline(source_w.value, chunk_w.value)

run_button.on_click(on_run_clicked)
display(run_button, output_area)

# %%
# [markdown]
# ## 📈 5. Observability & Visualization

# %%
def plot_summary():
    """
    Pulls loaded data and visualizes distributions.
    """
    engine = sa.create_engine(CONFIG["db_url"])
    df = pd.read_sql("SELECT * FROM clinical_data", engine)
    fig = px.histogram(df, x="bpm_category", color="bpm_category",
                       title="Heart Rate Categories Distribution")
    fig.show()

plot_summary()

# %%
# [markdown]
# ## 🧪 6. In-Notebook Testing

# %%
# Simple assertions to ensure pipeline correctness
engine = sa.create_engine(CONFIG["db_url"])
df_loaded = pd.read_sql("clinical_data", engine)
assert not df_loaded.empty, "No data loaded!"
assert set(["patient_id", "visit_date", "bpm_category", "lab_value_norm"]).issubset(df_loaded.columns), \
       "Missing expected columns!"
print("All tests passed ✅")

# %%
# [markdown]
# ## 🚀 Next Steps
#
# - Swap SQLite for PostgreSQL by updating `CONFIG["db_url"]`  
# - Integrate an Airflow DAG to schedule this notebook via Papermill  
# - Add error handling & dead-letter queue for bad records  
# - Extend transformations: pivot tables, time-series rolling stats  
# - Build a Voilà dashboard for real-time monitoring  
#
# —— End of notebook ——
