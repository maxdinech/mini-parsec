from pathlib import Path

from psycopg import Connection

from miniparsec import crypt
from miniparsec.paths import CLIENT_ROOT, SERVER_ROOT
from miniparsec.tokens import Token
from miniparsec.utils import console, file, folder, timing


class Scheme:
    def __init__(self, key: bytes, conn: Connection) -> None:
        self.conn: Connection = conn
        self.key: bytes = key
        self.protected_filenames: set[str]
        self.tables_names: set[str]
        self.newkey: bytes | None = None

    def reset(self) -> None:
        folder.empty(CLIENT_ROOT)
        folder.empty(SERVER_ROOT)

    def tokenize(self, word: str, prefix: str = "") -> Token:
        del word, prefix
        return Token()

    def add_token(self, token: Token, client_path: Path) -> None:
        del token, client_path
        pass

    def remove_token(self, token: Token, client_path: Path) -> None:
        del token, client_path
        console.log("No token removal method yet.")

    def search_token(
        self, token: Token, table_name: str
    ) -> set[str] | set[tuple[str, set[str]]]:
        del token, table_name
        return set()

    def search_word(self, word: str) -> set[str]:
        del word
        return set()

    def add_word(self, word: str, client_path: Path) -> None:
        token = self.tokenize(word)
        self.add_token(token, client_path)

    def remove_word(self, word: str, client_path: Path) -> None:
        token = self.tokenize(word)
        self.remove_token(token, client_path)

    def search_intersection(self, words: list[str]) -> set[str]:
        sets = [self.search_word(word) for word in words]
        return set.intersection(*sets)

    def search_union(self, words: list[str]) -> set[str]:
        sets = [self.search_word(word) for word in words]
        return set.union(*sets)

    def add_file_words(self, client_path: Path, verbose=True) -> int:
        del client_path, verbose
        return 0

    def remove_file_words(self, client_path: Path) -> None:
        del client_path
        pass

    def add_file(self, client_path: Path, verbose=True) -> tuple[int, float, float]:
        t1 = timing.timing(crypt.encrypt_file, verbose=False)(client_path, self.key)
        t2, count = timing.timing(self.add_file_words, verbose=False)(
            client_path, verbose=verbose
        )
        return t1, t2, count

    def remove_file(self, client_path: Path):
        server_path = file.get_server_path(client_path)
        temp_basename = f"temp_{client_path.name}"

        crypt.decrypt_file(server_path, self.key, temp_basename)
        temp_client_path = file.rename(client_path, temp_basename)
        self.remove_file_words(temp_client_path)
        # file.delete(temp_client_path)
        # file.delete(server_path)

    def merge(self) -> None:
        pass
