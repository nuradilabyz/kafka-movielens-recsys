"""Download and unpack the MovieLens 32M dataset into ./data/ml-32m/."""

from __future__ import annotations

import hashlib
import sys
import zipfile
from pathlib import Path

import requests

DATA_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
TARGET_DIR = Path(__file__).resolve().parent.parent / "data"
ZIP_PATH = TARGET_DIR / "ml-32m.zip"
EXTRACTED_DIR = TARGET_DIR / "ml-32m"


def _download(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        done = 0
        total_mb = total / (1024 * 1024) if total else 0
        with dest.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if not chunk:
                    continue
                f.write(chunk)
                done += len(chunk)
                done_mb = done / (1024 * 1024)
                if total:
                    pct = 100.0 * done / total
                    sys.stdout.write(f"\r  {pct:5.1f}%  {done_mb:8.1f}/{total_mb:.1f} MB")
                else:
                    sys.stdout.write(f"\r  downloaded {done_mb:8.1f} MB")
                sys.stdout.flush()
        sys.stdout.write("\n")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    required = ["ratings.csv", "movies.csv", "tags.csv", "links.csv"]
    if all((EXTRACTED_DIR / name).exists() for name in required):
        print(f"All MovieLens 32M files already exist in {EXTRACTED_DIR}; nothing to do.")
        return 0

    if not ZIP_PATH.exists():
        print(f"Downloading {DATA_URL}")
        _download(DATA_URL, ZIP_PATH)
        print(f"SHA-256: {_sha256(ZIP_PATH)}")
    else:
        print(f"Reusing existing archive {ZIP_PATH}")

    print(f"Extracting into {TARGET_DIR}")
    with zipfile.ZipFile(ZIP_PATH) as zf:
        zf.extractall(TARGET_DIR)

    missing = [name for name in required if not (EXTRACTED_DIR / name).exists()]
    if missing:
        print(f"ERROR: missing files after extraction: {missing}", file=sys.stderr)
        return 1

    print(f"Done. Files in {EXTRACTED_DIR}:")
    for name in required:
        size_mb = (EXTRACTED_DIR / name).stat().st_size / (1024 * 1024)
        print(f"  {name:15s} {size_mb:8.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
