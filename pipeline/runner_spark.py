import os
from tfx import v1 as tfx
from pipeline.pipeline import create_pipeline

PIPELINE_NAME   = "demo_tfx_spark"
PIPELINE_ROOT   = os.getenv("TFX_ARTIFACT_ROOT", "/artifacts")
DATA_ROOT       = "/data"
METADATA_PATH   = os.getenv("TFX_METADATA_PATH", "/metadata/metadata.db")
SERVING_DIR     = "/artifacts/serving/my_model"
JOB_ENDPOINT    = os.getenv("BEAM_JOB_ENDPOINT", "beam-jobserver:8099")

def run():
    pipeline = create_pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_root=os.path.join(PIPELINE_ROOT, PIPELINE_NAME),
        data_root=DATA_ROOT,
        metadata_path=METADATA_PATH,
        serving_model_dir=SERVING_DIR,
    )
    beam_args = [
        "--runner=PortableRunner",
        f"--job_endpoint={JOB_ENDPOINT}",
        "--environment_type=DOCKER",
        # SDK harness image must match your Beam version & Python version
        "--environment_config=apache/beam_python3.10_sdk:2.59.0",
    ]
    tfx.orchestration.beam.beam_dag_runner.BeamDagRunner(
        beam_pipeline_args=beam_args
    ).run(pipeline)

if __name__ == "__main__":
    run()
