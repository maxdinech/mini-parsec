import os
import zipfile

import psycopg
import requests
from rich.console import Console
from tqdm import tqdm

console = Console()


def download_database():
    path = "data/D357MB.zip"
    if not os.path.exists(path):
        url = "https://zenodo.org/record/3360392/files/D357MB.zip"
        console.log("Downloading databse...")
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


def create_tables(conn, scheme) -> None:
    cursor = conn.cursor()
    if scheme[:2] == "Pi":
        for tablename in ("edb", "edb2"):
            query = f"""
            CREATE TABLE {tablename} (
                token bytea,
                file bytea
            )"""
            cursor.execute(query)
        conn.commit()
    assert True, "No table descriptions provided for this scheme."


def reset_db_count():
    try:
        os.mkdir("data/client/")
    except FileExistsError:
        pass

    try:
        os.mkdir("data/server/")
    except FileExistsError:
        pass

    try:
        os.remove("data/client/db_count.pkl")
    except FileNotFoundError:
        pass

    try:
        os.remove("data/server/db_count")
    except FileNotFoundError:
        pass


def connect_db():
    conn = psycopg.connect(
        dbname="mini-parsec", host="localhost", user="admin", port="5432"
    )
    return conn


def reset_db(conn):
    cursor = conn.cursor()
    for tablename in ("edb", "edb2"):
        query = f"DROP TABLE {tablename}"
        try:
            cursor.execute(query)
        except psycopg.errors.UndefinedTable:
            conn.rollback()
    conn.commit()
