from pathlib import Path
import requests
from .line_reader import read_content

current_book_id = 0

def crawler():
    global current_book_id

    downloaded_book = 0
    while downloaded_book < 4:
        url = f"https://www.gutenberg.org/cache/epub/{current_book_id}/pg{current_book_id}.txt"
        response = requests.get(url)

        if response.status_code == 200:
            title = read_content(response.text)

            # Ir al directorio raíz del proyecto y luego a Datalake
            project_root = Path(__file__).resolve().parents[1]
            download_path = project_root / "Datalake"
            download_path.mkdir(exist_ok=True)

            file_name = f"book_{title}.txt"
            full_path = download_path / file_name

            with open(full_path, "w", encoding="utf-8") as file:
                file.write(response.text)

            print(f"✅ Guardado en: {full_path}")
            downloaded_book += 1

        current_book_id += 1
