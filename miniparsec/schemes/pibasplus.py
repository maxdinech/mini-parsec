from pathlib import Path

from psycopg import Connection, sql

from miniparsec import crypt, index
from miniparsec.paths import CLIENT_ROOT
from miniparsec.tokens import PiToken
from miniparsec.utils import console

from .pibas import PiBas


class PiBasPlus(PiBas):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.tables_names = ("edb", "edb2")
        self.protected_filenames = {"db_count.pkl"}

    def reset(self):
        super().reset()

    def tokenize(self, word: str) -> PiToken:
        return PiToken(
            crypt.hmac(f"1{word}", self.key),
            crypt.hmac(f"2{word}", self.key),
        )

    def search_token(self, token: PiToken) -> set[str]:
        return super().search_token(token)

    def add_word_helper(self, word: str, count: int, file_path: Path) -> None:
        cursor = self.conn.cursor()
        token = self.tokenize(word)

        query = sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(sql.Identifier("edb2"))

        entry_key = crypt.hmac(str(count), key=token.k1)
        path_str = str(file_path.relative_to(CLIENT_ROOT))
        entry_value = crypt.encrypt(path_str, key=token.k2)
        data = (entry_key, entry_value)

        cursor.execute(query, data)

    def add_word(self, word: str, client_path: Path) -> None:
        db_count = crypt.decrypt_dict_file("db_count", self.key)
        count = db_count.get(word, 0)

        self.add_word_helper(word, count, client_path)

        db_count[word] = count + 1
        crypt.encrypt_dict_file(db_count, "db_count", self.key)

    def add_file_words(self, client_path: Path) -> int:
        db_count = crypt.decrypt_dict_file("db_count", self.key)
        file_index, word_count = index.file_index(client_path)

        for word in file_index:
            count = db_count.get(word, 0)
            self.add_word_helper(word, count, client_path)
            db_count[word] = count + 1
        self.conn.commit()
        console.log(
            f"Words: {word_count:6,d}, "
            f"Unique words : {len(file_index):6,d}, "
            f"DB_count : {len(db_count):6,d}."
        )
        crypt.encrypt_dict_file(db_count, "db_count", self.key)

        return len(file_index)
