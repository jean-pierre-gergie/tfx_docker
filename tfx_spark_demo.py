import os
from tfx.orchestration.beam.beam_dag_runner import BeamDagRunner
from tfx.orchestration import pipeline as tfx_pipeline
from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from tfx.orchestration.metadata import sqlite_metadata_connection_config
from tfx.components import CsvExampleGen, StatisticsGen

# --- Paths (change if you want) ---
PIPELINE_NAME = "tfx_spark_demo"
HOME = os.path.expanduser("~")
PIPELINE_ROOT = os.path.join(HOME, "tfx", "pipelines", PIPELINE_NAME)
METADATA_PATH = os.path.join(HOME, "tfx", "metadata", PIPELINE_NAME, "metadata.db")
DATA_ROOT = os.path.join(HOME, "tfx", "data", "csv_demo")

os.makedirs(PIPELINE_ROOT, exist_ok=True)
os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)
os.makedirs(DATA_ROOT, exist_ok=True)

# Create a tiny CSV if none exists
csv_file = os.path.join(DATA_ROOT, "data.csv")
if not os.path.exists(csv_file):
    with open(csv_file, "w") as f:
        f.write("id,feature\n1,10\n2,20\n3,30\n")

# --- Components ---
example_gen = CsvExampleGen(input_base=DATA_ROOT)
stats_gen = StatisticsGen(examples=example_gen.outputs["examples"])

# --- Beam on Spark (PortableRunner via JobServer) ---
beam_pipeline_args = [
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK",
]

# --- MLMD (REQUIRED) ---
metadata_config = sqlite_metadata_connection_config(METADATA_PATH)

# --- Define TFX pipeline ---
def _build_pipeline():
    return tfx_pipeline.Pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_root=PIPELINE_ROOT,
        components=[example_gen, stats_gen],
        metadata_connection_config=metadata_config,   # << key fix
        enable_cache=True,
        beam_pipeline_args=beam_pipeline_args,
    )

if __name__ == "__main__":
    BeamDagRunner().run(_build_pipeline())
