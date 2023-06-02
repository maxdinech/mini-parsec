import argparse
import pickle
import re
import time
from dataclasses import dataclass
from typing import Literal

import nacl.secret
import nacl.utils
from nacl.hash import blake2b
from rich.console import Console
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from . import databases

console = Console()

Scheme = Literal["PiBas", "PiPack", "PiBasPlus", "PiBasDyn" "Sophos", "Diana"]


scheme: Scheme = "PiBasPlus"
keyword: bytes = b"test"
KEY: bytes = blake2b(keyword)[:32]
BOX = nacl.secret.SecretBox(KEY)


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


def tokenize_word(word: str) -> Token | None:
    if scheme[:2] == "Pi":
        return PiToken(
            blake2b(f"1{word}".encode("utf-8"), key=KEY),
            blake2b(f"2{word}".encode("utf-8"), key=KEY),
        )
    assert True, "No tokenization method provided for this scheme."


def search_token(conn, table, token: Token) -> list[str] | None:
    cursor = conn.cursor()
    if scheme[:2] == "Pi":
        key = blake2b(token.k2)[:32]
        box = nacl.secret.SecretBox(key)
        count = 0
        result = []
        while True:
            query = f"SELECT file FROM {table} WHERE token = %s;"
            query_key = blake2b(bytes(count), key=token.k1)
            data = (query_key,)
            cursor.execute(query, data)
            fetchone = cursor.fetchone()
            if fetchone is None:
                break
            path = box.decrypt(fetchone[0]).decode("utf-8")
            result.append(path)
            count += 1
        return result
    assert True, "No query method provided for this scheme."


def search_word(conn, word: str) -> list[str] | None:
    console.log(f"Searching word : '{word}' using scheme {scheme}.")
    if scheme[:2] == "Pi":
        t0 = time.time()
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
        console.log(f"Total: {len(result)} matches, in {time.time() - t0:.2f} seconds.")
    assert True, "No search method provided for this scheme."


def encrypt_file(path: str) -> None:
    dest_path = "data/server/" + path.split("client/")[-1]
    try:
        with open(path, "rb") as f:
            encrypted = BOX.encrypt(f.read())
            with open(dest_path, "wb") as ef:
                ef.write(encrypted)
    except FileNotFoundError:
        console.log("File to encrypt not found.")


def add_word(conn, word: str, count: int, path: str) -> list[str] | None:
    cursor = conn.cursor()
    if scheme[:2] == "Pi":
        token = tokenize_word(word)
        assert token is not None, "Empty token"

        query = f"INSERT INTO edb2 VALUES (%s, %s)"
        query_key = blake2b(bytes(count), key=token.k1)

        key = blake2b(token.k2)[:32]
        box = nacl.secret.SecretBox(key)
        query_value = box.encrypt(bytes(path, "utf-8"))

        data = (query_key, query_value)
        cursor.execute(query, data)
    assert True, "No add method provided for this scheme."


def add_file_index(conn, path: str) -> None:
    # Load DB_count
    try:
        with open("data/server/db_count", "rb") as ef:
            db_count_file = BOX.decrypt(ef.read())
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
    word_count = 0
    index = set()
    regex = re.compile("[^a-zA-Z]")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_clean = regex.sub(" ", line.lower())
            words = [w for w in line_clean.split(" ") if w]
            for word in words:
                index.add(word)
            word_count += len(words)

    for word in index:
        count = db_count.get(word, 0)
        add_word(conn, word, count, path)
        db_count[word] = count + 1
    conn.commit()
    console.log(
        f"Words: {word_count:6,d}, Unique words : {len(index):6,d}, DB_count : {len(db_count):6,d}"
    )

    # Save DB_count
    with open("data/client/db_count.pkl", "wb") as f:
        pickle.dump(db_count, f)
    try:
        with open("data/client/db_count.pkl", "rb") as f:
            db_count_file = BOX.encrypt(f.read())
            with open("data/server/db_count", "wb") as ef:
                ef.write(db_count_file)
    except FileNotFoundError:
        pass


def add_file(conn, path: str) -> tuple[float, float]:
    t0 = time.time()
    encrypt_file(path)
    t1 = time.time() - t0
    add_file_index(conn, path)
    t2 = time.time() - t0 - t1
    return (t1, t2)


class Watcher:
    def __init__(self, directory, handler):
        self.observer = Observer()
        self.handler = handler
        self.directory = directory

    def run(self):
        self.observer.schedule(self.handler, self.directory, recursive=True)
        self.observer.start()
        console.log(f"Watcher Running in {self.directory}.")
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
        self.observer.join()
        console.log("Watcher Terminated.")


class MyHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.event_type == "created":
            console.log(event)
            path = event.src_path
            filename = path.split("/")[-1]
            if filename != "db_count.pkl":
                console.log(f"Adding file '{path}'")
                t1, t2 = add_file(conn, path)
                console.log(f"Encryption : {t1:.2f}s, indexing : {t2:.2f}s\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Mini-Parsec",
        description="Mini-Parsec : client et recherche.",
    )
    _ = parser.add_argument("mode", nargs="?", default="sync")
    _ = parser.add_argument("--delete", help="delete files", action="store_true")
    _ = parser.add_argument("--query", type=str, help="search term", default="")

    args = parser.parse_args()

    if args.mode == "sync":
        databases.download_gutenberg_database()
        databases.download_enron_database()

        conn = databases.connect_db()
        if args.delete:
            console.log("Clearing database.")
            databases.reset_db(conn)
            databases.reset_db_count()
            console.log("Deleting local files.")
            databases.create_tables(conn, scheme)

        w = Watcher("data/client/", MyHandler())
        w.run()

    if args.mode == "search":
        conn = databases.connect_db()
        query = args.query
        search_word(conn, query)
