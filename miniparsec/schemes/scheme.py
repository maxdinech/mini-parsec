import time

from psycopg import Connection

from miniparsec import crypt
from miniparsec.tokens import Token
from miniparsec.utils import folder


class Scheme:
    def __init__(self, key: bytes, conn: Connection) -> None:
        self.conn: Connection = conn
        self.key: bytes = key
        self.protected_filenames: set[str]

    def reset(self) -> None:
        folder.empty("data/client")
        folder.empty("data/server")

    def tokenize(self, word: str) -> Token:
        del word
        return Token()

    def add_token(self, token: Token) -> None:
        del token
        pass

    def search_token(self, token: Token) -> set[str]:
        del token
        return set()

    def search_word(self, word: str) -> set[str]:
        token = self.tokenize(word)
        return self.search_token(token)

    def add_word(self, word: str) -> None:
        token = self.tokenize(word)
        self.add_token(token)

    def search_intersection(self, words: list[str]) -> set[str]:
        sets = [self.search_word(word) for word in words]
        return set.intersection(*sets)

    def search_union(self, words: list[str]) -> set[str]:
        sets = [self.search_word(word) for word in words]
        return set.union(*sets)

    def add_file_words(self, file_path: str) -> None:
        del file_path
        pass

    def add_file(self, file_path: str) -> tuple[float, float]:
        t0 = time.time()
        crypt.encrypt_file(file_path, self.key)
        delta1 = time.time() - t0
        t0 = time.time()
        self.add_file_words(file_path)
        delta2 = time.time() - t0
        return delta1, delta2
