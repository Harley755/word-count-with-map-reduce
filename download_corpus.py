# 01_download.py
# Télécharge le corpus Gutenberg (~1 Go) depuis linux

import urllib.request
import os
import time

os.makedirs("data", exist_ok=True)
OUTPUT = "data/corpus.txt"

BOOK_IDS = [
    1342, 84, 1661, 74, 11, 1952, 2701, 46, 1400, 345,
    1232, 2542, 98, 158, 1080, 36, 5200, 2554, 2600, 76,
    100, 1260, 2591, 2814, 1184, 203, 2413, 16, 2148, 514,
    244, 1727, 4300, 766, 768, 2097, 996, 600, 730, 863,
    174, 1998, 2500, 3207, 120, 408, 2680, 43, 35, 25344,
    55, 135, 219, 541, 580, 932, 1251, 1399, 1497,
    1690, 2003, 2400, 6130, 7849, 8800, 10007, 11308,
    14838, 16328, 17489, 19942, 20748, 22120, 23684,
    25307, 27827, 30254, 33283, 36034, 42671, 45368,
]

with open(OUTPUT, "w", encoding="utf-8") as out:
    for i, book_id in enumerate(BOOK_IDS):
        url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                text = response.read().decode("utf-8", errors="ignore")
                out.write(text)
                out.write("\n")
            size_mb = os.path.getsize(OUTPUT) / 1e6
            print(f"[{i+1:3}/{len(BOOK_IDS)}] ID={book_id:<6}  corpus={size_mb:.1f} Mo")
        except Exception as e:
            print(f"[{i+1:3}/{len(BOOK_IDS)}] ID={book_id:<6}  ERREUR: {e}")
        time.sleep(0.3)   # pause pour ne pas se faire bloquer

final_size = os.path.getsize(OUTPUT) / 1e9
print(f"\n[OK] Corpus prêt : {OUTPUT}  ({final_size:.2f} Go)")