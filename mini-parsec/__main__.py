import pickle
import re
from dataclasses import dataclass
from typing import Literal

import nacl.secret
import nacl.utils
from nacl.hash import blake2b
from rich.console import Console

from . import databases

console = Console()

Scheme = Literal["PiBas", "PiPack", "PiBasPlus", "PiBasDyn" "Sophos", "Diana"]


# Params

scheme: Scheme = "PiBas"
keyword: bytes = b"test"
KEY: bytes = blake2b(keyword)[:32]
BOX = nacl.secret.SecretBox(KEY)


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


def add_file(conn, path: str) -> None:
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
            db_count_file = BOX.encrypt(f.read())
            with open("data/server/db_count", "wb") as ef:
                ef.write(db_count_file)
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    databases.download_database()

    conn = databases.connect_db()
    databases.reset_db(conn)
    databases.reset_db_count()
    databases.create_tables(conn, scheme)

    count = 0
    for i in range(10):
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
