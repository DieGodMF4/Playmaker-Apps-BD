import logging
from pathlib import Path
from src.search_project.control.orchestrator import control_pipeline

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # descarga incremental: intenta añadir 50 libros nuevos (configurable)
    control_pipeline(target_new_downloads=50, datalake_root=Path("data/datalake"), raw_root=Path("data/raw"))
    print("Pipeline step finished. Re-run to continue indexing/downloading.")
