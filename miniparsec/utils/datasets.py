import os
import tarfile
import zipfile
from pathlib import Path

import requests
from tqdm import tqdm

from . import console


def download_file(url: str, path: Path) -> None:
    response = requests.get(url, stream=True)
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(path, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        print("ERROR, something went wrong")
    console.log("Done.")


def unzip_file(path: Path, dest: Path) -> None:
    console.log("Unzipping...")
    with zipfile.ZipFile(path, "r") as zip_ref:
        zip_ref.extractall(dest)
    console.log("Done.")


def untar_file(path: Path, dest: Path) -> None:
    console.log("Unpacking...")
    tar = tarfile.open(path, "r:gz")
    for tarinfo in tar:
        tar.extract(tarinfo, dest)
    tar.close()
    console.log("Done.")


def download_gutenberg(size: str = "357MB"):
    path = Path(f"data/D{size}.zip")
    if not os.path.exists(path):
        url = f"https://zenodo.org/record/3360392/files/D{size}.zip"
        console.log(f"Downloading Gutenberg {size} dataset...")
        download_file(url, path)
        unzip_file(path, Path("data/Enron"))


def download_enron():
    path = Path("data/enron_mail_20150507.tar.gz")
    if not os.path.exists(path):
        url = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"
        download_file(url, path)
        untar_file(path, Path("data/"))


def download_corpora(count: int = 0) -> None:
    path = Path("data/Corpora/{count:04d}")
    if not os.path.exists(path):
        url = f"https://digitalcorpora.s3.amazonaws.com/corpora/files/CC-MAIN-2021-31-PDF-UNTRUNCATED/zipfiles/0000-0999/{count:04d}.zip"
        download_file(url, path)
        unzip_file(path, Path("data/Corpora"))
