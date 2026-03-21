"""
MAIN RUNNER - PhD Level Data Engineering Platform

Usage:
    python run.py generate      # Generate synthetic data
    python run.py etl           # Run ETL pipeline
    python run.py quality       # Run data quality audit
    python run.py scd           # Run SCD Type 2 demo
    python run.py stream        # Run stream processing simulation
    python run.py pipeline      # Run full DAG-orchestrated pipeline
    python run.py serve         # Start the API server + dashboard
    python run.py all           # Pipeline + Serve
"""

import subprocess
import sys
import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)


def run_generate():
    print("\n[Step 1] Generating synthetic data...\n")
    from src.etl.generate_data import main as gen_main
    gen_main()


def run_etl():
    print("\n[Step 2] Running ETL pipeline...\n")
    from src.etl.etl_pipeline import ETLPipeline
    pipeline = ETLPipeline()
    pipeline.run()


def run_quality():
    print("\n[Step 3] Running data quality audit...\n")
    from src.etl.data_quality import DataQualityEngine
    engine = DataQualityEngine()
    engine.run_full_audit()


def run_scd():
    print("\n[Step 4] Running SCD Type 2 processing...\n")
    from src.etl.scd_handler import SCDHandler
    handler = SCDHandler()
    handler.setup_scd_tables()
    handler.initial_load_customers()
    handler.initial_load_products()
    handler.simulate_changes()


def run_stream():
    print("\n[Step 5] Running stream processing simulation...\n")
    from src.etl.stream_processor import StreamProcessor
    processor = StreamProcessor()
    processor.run_simulation(num_batches=5, batch_size=200)


def run_api_ingestion():
    print("\n[Step 6] Ingesting public API data...\n")
    from src.etl.api_ingestion import run_all_ingestions
    run_all_ingestions()


def run_full_pipeline():
    """Execute the full pipeline using DAG orchestration."""
    from src.etl.pipeline_orchestrator import PipelineDAG, DAGTask

    dag = PipelineDAG(
        dag_id="data_engineering_pipeline_v3",
        description="Full data engineering pipeline with quality, SCD, streaming, and live API ingestion"
    )

    # Define tasks
    dag.add_task(DAGTask("generate_data", run_generate, "Generate synthetic raw data", retries=1))
    dag.add_task(DAGTask("etl_pipeline", run_etl, "Extract, Transform, Load to warehouse", retries=1))
    dag.add_task(DAGTask("data_quality", run_quality, "Run data quality audit", retries=0))
    dag.add_task(DAGTask("scd_processing", run_scd, "Apply SCD Type 2 changes", retries=0))
    dag.add_task(DAGTask("stream_processing", run_stream, "Run stream processing simulation", retries=0))
    dag.add_task(DAGTask("api_ingestion", run_api_ingestion, "Ingest data from public APIs", retries=1))

    # Define dependencies (DAG structure)
    # generate_data -> etl_pipeline -> [data_quality, scd_processing, stream_processing, api_ingestion]
    dag.set_dependency("etl_pipeline", "generate_data")
    dag.set_dependency("data_quality", "etl_pipeline")
    dag.set_dependency("scd_processing", "etl_pipeline")
    dag.set_dependency("stream_processing", "etl_pipeline")
    dag.set_dependency("api_ingestion", "etl_pipeline")

    # Execute the DAG
    success = dag.execute()

    # Save lineage
    lineage_path = os.path.join(BASE_DIR, 'data', 'processed', 'pipeline_lineage.json')
    os.makedirs(os.path.dirname(lineage_path), exist_ok=True)
    with open(lineage_path, 'w') as f:
        json.dump({'dag_id': dag.dag_id, 'lineage': dag.get_lineage()}, f, indent=2)

    print(f"\nLineage saved: {lineage_path}")
    return success


def run_serve():
    print("\n[Server] Starting API server...\n")
    print("Dashboard: http://localhost:8000")
    print("API Docs:  http://localhost:8000/docs")
    print("Press Ctrl+C to stop.\n")
    subprocess.run([
        sys.executable, '-m', 'uvicorn',
        'src.api.main:app',
        '--host', '0.0.0.0',
        '--port', '8000',
        '--reload'
    ], cwd=BASE_DIR)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()
    commands = {
        'generate': run_generate,
        'etl': run_etl,
        'quality': run_quality,
        'scd': run_scd,
        'stream': run_stream,
        'ingest': run_api_ingestion,
        'pipeline': run_full_pipeline,
        'serve': run_serve,
        'all': lambda: (run_full_pipeline(), run_serve()),
    }

    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
