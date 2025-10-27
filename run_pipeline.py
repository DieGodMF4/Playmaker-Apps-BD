# run_pipeline.py
import logging
from pathlib import Path
from src.search_project.control.orchestrator import control_pipeline

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    datalake_root = Path("data/datalake")
    raw_root = Path("data/raw")

    # Intenta descargar e indexar todo en una sola ejecución.
    # control_pipeline() devuelve True cuando ha habido progreso (p.ej., nuevas descargas)
    # y pide al caller repetir para que se indexe inmediatamente.
    max_rounds = 2   # tope de seguridad para evitar bucles infinitos
    rounds = 0

    while True:
        rounds += 1
        progressed = control_pipeline(
            target_new_downloads=10,          # ajusta si quieres
            datalake_root=datalake_root,
            raw_root=raw_root
        )
        if not progressed or rounds >= max_rounds:
            break

    logging.info("✅ Pipeline completo: descargas e indexación finalizadas (o no hay más progreso).")

if __name__ == "__main__":
    main()
