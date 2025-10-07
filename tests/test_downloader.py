from pathlib import Path
from src.search_project.crawler.downloader import download_book


def test_download_small(tmp_path):
    # This test will attempt to download a real Gutenberg book (e.g., 1342).
    # You can mark this as integration test in CI.
    dl_root = tmp_path / "datalake"
    ctrl = tmp_path / "control"
    raw = tmp_path / "raw"
    ok = download_book(1342, datalake_root=dl_root, control_dir=ctrl, alt_raw_root=raw, max_retries=1, timeout=10)
    # If Gutenberg is reachable the book should be downloaded and files created
    assert (ok and any(dl_root.rglob("1342.body.txt")))
