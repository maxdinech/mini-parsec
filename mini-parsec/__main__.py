import argparse
import os
import pickle
import re
import time

import nacl.secret
import nacl.utils
from nacl.hash import blake2b
from psycopg import Connection, sql
from rich.console import Console
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from . import databases, datasets, schemes, tokens
from .schemes import Scheme
from .tokens import Token

console = Console()


def tokenize_word(word: str, key: bytes, scheme: Scheme) -> Token | None:
    if isinstance(scheme, schemes.PiBasPlus):
        return tokens.PiToken(
            blake2b(f"1{word}".encode("utf-8"), key=key),
            blake2b(f"2{word}".encode("utf-8"), key=key),
        )
    assert True, "No tokenization method provided for this scheme."


def search_token(
    conn: Connection, table: str, token: Token, scheme: Scheme
) -> list[str] | None:
    cursor = conn.cursor()
    if isinstance(scheme, schemes.PiBasPlus):
        assert isinstance(token, tokens.PiToken), "Invalid token for this scheme."
        key = blake2b(token.k2)[:32]
        box = nacl.secret.SecretBox(key)
        count = 0
        result = []
        while True:
            query = sql.SQL("SELECT file FROM {} WHERE token = %s;").format(
                sql.Identifier(table)
            )
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


def search_word(conn: Connection, word: str, key, scheme: Scheme) -> list[str] | None:
    console.log(f"Searching word : '{word}' using scheme {scheme}.")
    if isinstance(scheme, schemes.PiBasPlus):
        t0 = time.time()
        token = tokenize_word(word.lower(), key, scheme)
        assert isinstance(token, tokens.PiToken), "Invalid token for this scheme."

        r1 = search_token(conn, "edb", token, scheme)
        r2 = search_token(conn, "edb2", token, scheme)

        result = []
        if r1 is not None:
            result += r1
        if r2 is not None:
            result += r2

        console.log(f"Total: {len(result)} matches, in {time.time() - t0:.2f} seconds.")
        return result

    assert True, "No search method provided for this scheme."


def encrypt_file(path: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    dest_path = "data/server/" + path.split("client/")[-1]
    try:
        with open(path, "rb") as f:
            encrypted = box.encrypt(f.read())
            with open(dest_path, "wb") as ef:
                ef.write(encrypted)
    except FileNotFoundError:
        console.log("File to encrypt not found.")


def add_word(
    conn: Connection, word: str, count: int, path: str, key, scheme: Scheme
) -> list[str] | None:
    cursor = conn.cursor()
    if isinstance(scheme, schemes.PiBasPlus):
        token = tokenize_word(word, key, scheme)
        assert isinstance(token, tokens.PiToken), "Invalid token for this scheme."
        assert token is not None, "Empty token"

        query = sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(sql.Identifier("edb2"))

        query_key = blake2b(bytes(count), key=token.k1)
        key = blake2b(token.k2)[:32]
        box = nacl.secret.SecretBox(key)
        query_value = box.encrypt(bytes(path, "utf-8"))

        data = (query_key, query_value)
        cursor.execute(query, data)
    assert True, "No add method provided for this scheme."


def add_file_index(
    conn: Connection, path: str, key: bytes, scheme: Scheme, min_length: int = 3
) -> None:
    """Indexation d'un fichier.

    Args:
        conn: Connexion PostgreSQL
        path: Chemin du fichier
        scheme: Schéma de chiffrement
        key: Clé de chiffrement
        min_length: Longueur monimale de mot
    """
    box = nacl.secret.SecretBox(key)
    if isinstance(scheme, schemes.PiBasPlus):
        # Load DB_count
        try:
            with open("data/server/db_count", "rb") as ef:
                db_count_file = box.decrypt(ef.read())
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
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                words = re.findall(r"\w+", line.lower())
                words = set(w for w in words if len(w) >= min_length)
                index.update(words)
                word_count += len(words)

        for word in index:
            count = db_count.get(word, 0)
            add_word(conn, word, count, path, key, scheme)
            db_count[word] = count + 1
        conn.commit()
        console.log(
            f"Words: {word_count:6,d}, "
            f"Unique words : {len(index):6,d}, "
            f"DB_count : {len(db_count):6,d}."
        )

        # Save DB_count
        with open("data/client/db_count.pkl", "wb") as f:
            pickle.dump(db_count, f)
        try:
            with open("data/client/db_count.pkl", "rb") as f:
                db_count_file = box.encrypt(f.read())
                with open("data/server/db_count", "wb") as ef:
                    ef.write(db_count_file)
        except FileNotFoundError:
            pass


def add_file(conn: Connection, path: str, key: bytes) -> tuple[float, float]:
    """Ajout d'un fichier au serveur.

    Args:
        conn: Connexion PostgreSQL
        path: Chemin du fichier
        key: Clé de chiffrement
    """
    t0 = time.time()
    encrypt_file(path, key)
    t1 = time.time() - t0
    add_file_index(conn, path, key, scheme)
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
        console.log(f"Watcher Running in {self.directory}. You can add files now.")
        try:
            while True:
                time.sleep(1)
        except:
            self.observer.stop()
        self.observer.join()
        console.log("Watcher Terminated.")


class MyHandler(FileSystemEventHandler):
    def __init__(self, key: bytes) -> None:
        self.key: bytes = key

    def on_any_event(self, event):
        if event.event_type == "created":
            path = event.src_path
            if event.is_directory:
                server_path = "data/server/" + path.split("client/")[-1]
                try:
                    os.mkdir(server_path)
                except FileExistsError:
                    pass
            else:
                filename = path.split("/")[-1]
                if filename != "db_count.pkl":
                    console.log(f"Adding file '{path}'")
                    try:
                        t1, t2 = add_file(conn, path, self.key)
                    except UnicodeDecodeError:
                        console.log(f"Error: Failed to decode file {path}")
                    else:
                        console.log(f"Encryption: {t1:.2f}s, indexing: {t2:.2f}s\n")


if __name__ == "__main__":
    scheme: Scheme = schemes.PiBasPlus()
    parser = argparse.ArgumentParser(
        prog="Mini-Parsec",
        description="Mini-Parsec : client et recherche.",
    )
    _ = parser.add_argument("mode", nargs="?", default="sync")
    _ = parser.add_argument("-K", "--key", type=str, help="search term", required=True)
    _ = parser.add_argument("-r-", "--reset", help="reset server", action="store_true")
    _ = parser.add_argument("-s", "--show", help="show results", action="store_true")
    _ = parser.add_argument("-q", "--query", type=str, help="search term", default="")
    _ = parser.add_argument("-K2", "--newkey", type=str, help="new key", default="")

    args = parser.parse_args()

    keyword: bytes = bytes(args.key, "utf-8")
    KEY: bytes = blake2b(keyword)[:32]

    match args.mode:
        case "dataset":
            datasets.download_gutenberg_database()
            datasets.download_enron_database()

        case "server":
            conn = databases.connect_db()
            if args.reset:
                console.log("Clearing database.")
                databases.reset_db(conn)
                databases.reset_db_count()
                console.log("Deleting local files.")
                databases.create_tables(conn, scheme)

            w = Watcher("data/client/", MyHandler(KEY))
            w.run()

        case "repack":
            conn = databases.connect_db()
            new_keyword = bytes(args.newkey, "utf-8")
            if new_keyword != keyword:
                NEW_KEY: bytes = blake2b(keyword)[:32]
                # Stuff here
                keyword, KEY = new_keyword, NEW_KEY

            console.log("Repack done.")

        case "search":
            conn = databases.connect_db()
            query = args.query
            words = query.split("+")
            results = None
            results = [search_word(conn, word, KEY, scheme) for word in words]
            results = [set(r) for r in results if r is not None]
            intersection = set.intersection(*results)
            if args.show:
                console.log(intersection)
            console.log(f"Intersection: {len(intersection)} matches.")

        case _:
            console.log("Invalid mode.")
