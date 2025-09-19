from tfx import v1 as tfx
import os

def create_pipeline(
    pipeline_name: str,
    pipeline_root: str,
    data_root: str,
    metadata_path: str,
    serving_model_dir: str,
):
    example_gen = tfx.components.CsvExampleGen(input_base=data_root)
    statistics_gen = tfx.components.StatisticsGen(examples=example_gen.outputs["examples"])
    schema_gen = tfx.components.SchemaGen(statistics=statistics_gen.outputs["statistics"], infer_feature_shape=True)
    example_validator = tfx.components.ExampleValidator(
        statistics=statistics_gen.outputs["statistics"], schema=schema_gen.outputs["schema"]
    )
    transform = tfx.components.Transform(
        examples=example_gen.outputs["examples"],
        schema=schema_gen.outputs["schema"],
        module_file=os.path.join(os.getcwd(), "pipeline", "preprocessing.py"),
    )
    trainer = tfx.components.Trainer(
        module_file=os.path.join(os.getcwd(), "pipeline", "trainer.py"),
        examples=transform.outputs["transformed_examples"],
        transform_graph=transform.outputs["transform_graph"],
    )
    eval_config = tfx.proto.EvalConfig(
        model_specs=[tfx.proto.ModelSpec(label_key="label")],
        metrics_specs=[tfx.proto.MetricsSpec()],
    )
    evaluator = tfx.components.Evaluator(
        examples=example_gen.outputs["examples"],
        model=trainer.outputs["model"],
        eval_config=eval_config,
    )
    pusher = tfx.components.Pusher(
        model=trainer.outputs["model"],
        push_destination=tfx.proto.PushDestination(
            filesystem=tfx.proto.PushDestination.Filesystem(base_directory=serving_model_dir)
        ),
    )
    return tfx.dsl.Pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        components=[example_gen, statistics_gen, schema_gen, example_validator, transform, trainer, evaluator, pusher],
        metadata_connection_config=tfx.orchestration.metadata.sqlite_metadata_connection_config(metadata_path),
        enable_cache=True,
    )
