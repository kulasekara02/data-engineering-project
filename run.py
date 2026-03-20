"""
MAIN RUNNER - Executes the full data engineering pipeline and starts the API.

Usage:
    python run.py generate    # Generate synthetic data
    python run.py etl         # Run ETL pipeline
    python run.py serve       # Start the API server
    python run.py all         # Generate + ETL + Serve
"""

import subprocess
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def run_generate():
    print("\n🔹 Step 1: Generating synthetic data...\n")
    subprocess.run([sys.executable, os.path.join(BASE_DIR, 'src', 'etl', 'generate_data.py')], check=True)


def run_etl():
    print("\n🔹 Step 2: Running ETL pipeline...\n")
    subprocess.run([sys.executable, os.path.join(BASE_DIR, 'src', 'etl', 'etl_pipeline.py')], check=True)


def run_serve():
    print("\n🔹 Step 3: Starting API server...\n")
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

    if command == 'generate':
        run_generate()
    elif command == 'etl':
        run_etl()
    elif command == 'serve':
        run_serve()
    elif command == 'all':
        run_generate()
        run_etl()
        run_serve()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == '__main__':
    main()
