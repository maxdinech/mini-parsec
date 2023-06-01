from tqdm import tqdm
import requests
import zipfile
import hashlib
import os
import pickle
from cryptography.fernet import Fernet
import re
import base64
from typing import Literal
import psycopg
import hmac

from rich.console import Console
import testing.postgresql
from sqlalchemy import create_engine
from dataclasses import dataclass

console = Console()

Scheme = Literal["PiBas", "PiPack", "PiBasPlus", "PiBasDyn" "Sophos", "Diana"]


# Params

scheme: Scheme = "PiBas"
KEY: bytes = b"e8q-TDOEho--oXF99dkIM6XERuXsxdDZpopvqYc4h-0="
new_KEY: bytes = b"e8q-TDOEho--oXF99dkIM6XERuXsxdDZpopvqYc4h-0="


@dataclass
class PiToken:
    k1: bytes
    k2: bytes


@dataclass
class SophosToken:
    k1: bytes
    k2: bytes


@dataclass
class DianaToken:
    k1: bytes
    k2: bytes


Token = PiToken | SophosToken | DianaToken


def read_db_count() -> dict:
    return {}


def write_db_count() -> None:
    return


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


def create_tables(conn) -> None:
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


def hmac2(key: bytes, string: str) -> bytes:
    digestmod = "SHA-1"
    return hmac.new(key, string.encode(), digestmod=digestmod).digest()


def tokenize_word(word: str) -> Token | None:
    if scheme[:2] == "Pi":
        return PiToken(hmac2(KEY, f"1{word}"), hmac2(KEY, f"2{word}"))
    assert True, "No tokenization method provided for this scheme."


def search_token(conn, table, token: Token) -> list[str] | None:
    cursor = conn.cursor()
    if scheme[:2] == "Pi":
        fernet = Fernet(gen_fernet_key(token.k2))
        count = 0
        result = []
        while True:
            query = f"SELECT file FROM {table} WHERE token = %s;"
            query_key = hmac2(token.k1, str(count))
            data = (query_key,)
            cursor.execute(query, data)
            fetchone = cursor.fetchone()
            if fetchone is None:
                break
            path = fernet.decrypt(fetchone[0]).decode("utf-8")
            result.append(path)
            count += 1
        return result
    assert True, "No query method provided for this scheme."


def search_word(conn, word: str) -> list[str] | None:
    console.log(f"Searching word : '{word}' using scheme {scheme}.")
    if scheme[:2] == "Pi":
        token = tokenize_word(word.lower())
        assert token is not None, "Empty token"
        r1 = search_token(conn, "edb", token)
        console.log(f"EDB results : {r1}")
        r2 = search_token(conn, "edb2", token)
        console.log(f"EDB' results : {r2}")
        result = []
        if r1 is not None:
            result += r1
        if r2 is not None:
            result += r2
    assert True, "No search method provided for this scheme."


def gen_fernet_key(passcode: bytes) -> bytes:
    assert isinstance(passcode, bytes)
    hlib = hashlib.md5()
    hlib.update(passcode)
    return base64.urlsafe_b64encode(hlib.hexdigest().encode("utf-8"))


def add_word(conn, word: str, count: int, path: str) -> list[str] | None:
    cursor = conn.cursor()
    if scheme[:2] == "Pi":
        token = tokenize_word(word)
        assert token is not None, "Empty token"
        query = f"INSERT INTO edb2 VALUES (%s, %s)"
        query_key = hmac2(token.k1, str(count))

        fernet = Fernet(gen_fernet_key(token.k2))
        query_value = fernet.encrypt(bytes(path, "utf-8"))

        hmac2(token.k1, str(count))
        data = (query_key, query_value)
        cursor.execute(query, data)
    assert True, "No add method provided for this scheme."


def add_file(conn, path: str) -> None:
    # Load DB_count
    try:
        with open("data/server/db_count", "rb") as ef:
            db_count_file = Fernet(KEY).decrypt(ef.read())
            with open("data/client/db_count.pkl", "wb") as f:
                f.write(db_count_file)
    except FileNotFoundError:
        pass

    try:
        with open("data/client/db_count.pkl", "rb") as f:
            db_count = pickle.load(f)
    except FileNotFoundError:
        db_count = {}

    # Build file index
    index = set()
    regex = re.compile("[^a-zA-Z]")
    with open(path, "r") as f:
        for line in f:
            line_clean = regex.sub(" ", line).lower()
            words = [w for w in line_clean.split(" ") if w]
            for word in words:
                index.add(word)

    for word in index:
        count = db_count.get(word, 0)
        add_word(conn, word, count, path)
        db_count[word] = count + 1
    conn.commit()

    # Save DB_count
    with open("data/client/db_count.pkl", "wb") as f:
        pickle.dump(db_count, f)
    try:
        with open("data/client/db_count.pkl", "rb") as f:
            db_count_file = Fernet(KEY).encrypt(f.read())
            with open("data/server/db_count", "wb") as ef:
                ef.write(db_count_file)
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    download_database()

    conn = connect_db()
    reset_db(conn)
    reset_db_count()
    create_tables(conn)

    count = 0
    for i in range(100):
        path = f"data/D357MB/{i}.txt"
        try:
            add_file(conn, path)
        except FileNotFoundError:
            pass
        else:
            console.log(f"Adding file {path}")
            count += 1
    console.log(f"Added {count} files.")

    search_word(conn, "and")
    search_word(conn, "pull")
    search_word(conn, "ache")

    for i in range(100, 200):
        path = f"data/D357MB/{i}.txt"
        try:
            add_file(conn, path)
        except FileNotFoundError:
            pass
        else:
            console.log(f"Adding file {path}")
            count += 1
    console.log(f"Added {count} files.")

    search_word(conn, "and")
    search_word(conn, "pull")
    search_word(conn, "ache")
