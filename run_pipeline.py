import argparse
import logging
from pathlib import Path
from src.search_project.control.orchestrator import control_pipeline


def parse_args():
    p = argparse.ArgumentParser(description="End-to-end crawler → metadata → inverted index")
    p.add_argument("--target-new", type=int, default=10,
                   help="How many NEW downloads to try this session (default: 10). 0 means only index pending.")
    p.add_argument("--max-tries", type=int, default=100000,
                   help="Upper bound on random ID attempts while downloading (default: 100000).")
    p.add_argument("--loop", action="store_true",
                   help="Keep looping until the session target is met and there is nothing left to index.")
    p.add_argument("--datalake", type=Path, default=Path("data/datalake"))
    p.add_argument("--raw", type=Path, default=Path("data/raw"))
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    # One pass, or keep looping if --loop
    while True:
        progress = control_pipeline(
            target_new_downloads=args.target_new,
            datalake_root=args.datalake,
            raw_root=args.raw,
            total_tries=args.max_tries,
        )
        if not args.loop or not progress:
            break

    logging.info("Pipeline finished.")
