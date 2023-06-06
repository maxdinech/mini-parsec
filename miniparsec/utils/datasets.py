import os
import tarfile
import zipfile

import requests
from rich.console import Console
from tqdm import tqdm

console = Console()


def download_gutenberg_database(size: str = "357MB"):
    path = f"data/D{size}.zip"
    if not os.path.exists(path):
        url = f"https://zenodo.org/record/3360392/files/D{size}.zip"
        console.log(f"Downloading Gutenberg {size} dataset...")
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

        console.log("Unzipping...")
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall("data")
        console.log("Done.")


def download_enron_database():
    path = "data/enron_mail_20150507.tar.gz"
    if not os.path.exists(path):
        url = "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"
        console.log("Downloading Enron Email dataset...")
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

        console.log("Unpacking...")
        tar = tarfile.open(path, "r:gz")
        for tarinfo in tar:
            tar.extract(tarinfo, "data/Enron/")
        tar.close()
        console.log("Done.")
