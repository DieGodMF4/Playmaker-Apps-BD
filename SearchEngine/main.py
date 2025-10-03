from pathlib import Path
from SearchEngine.indexer.indexer_json import build_index

if __name__ == "__main__":
    datalake_path = Path("data/datalake")
    output_file = Path("data/datamarts/inverted_index.json")
    build_index(datalake_path, output_file)

