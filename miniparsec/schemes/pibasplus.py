from pathlib import Path

from nacl.hash import blake2b
from psycopg import Connection, sql

from miniparsec import crypt, databases, index
from miniparsec.tokens import PiToken
from miniparsec.utils import console

from .scheme import Scheme


class PiBasPlus(Scheme):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.protected_filenames = {"db_count.pkl"}

    def reset(self):
        super().reset()
        table_names = ("edb", "edb2")
        for table_name in table_names:
            databases.drop_table(self.conn, table_name)
            databases.create_table(
                self.conn, table_name, {"token": "bytea", "file": "bytea"}
            )
            databases.create_index(self.conn, table_name)

    def tokenize(self, word: str) -> PiToken:
        return PiToken(
            crypt.hmac(f"1{word}", self.key),
            crypt.hmac(f"2{word}", self.key),
        )

    def search_token(self, token: PiToken) -> set[str]:
        cursor = self.conn.cursor()
        result = set()
        for table_name in ("edb", "edb2"):
            count = 0
            while True:
                query = sql.SQL("SELECT file FROM {} WHERE token = %s;").format(
                    sql.Identifier(table_name)
                )
                query_key = crypt.hmac(str(count), token.k1)
                data = (query_key,)
                cursor.execute(query, data)
                fetchone = cursor.fetchone()
                if fetchone is None:
                    break
                path = crypt.decrypt(fetchone[0], key=token.k2)
                result.add(path)
                count += 1
        return result

    def add_word_helper(self, word: str, count: int, file_path: Path) -> None:
        cursor = self.conn.cursor()
        token = self.tokenize(word)

        query = sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(sql.Identifier("edb2"))

        entry_key = crypt.hmac(str(count), key=token.k1)
        entry_value = crypt.encrypt(str(file_path), key=token.k2)
        data = (entry_key, entry_value)

        cursor.execute(query, data)

    def add_word(self, word: str, client_path: Path) -> None:
        db_count = crypt.decrypt_dict_file("db_count", self.key)
        count = db_count.get(word, 0)

        self.add_word_helper(word, count, client_path)

        db_count[word] = count + 1
        crypt.encrypt_dict_file(db_count, "db_count", self.key)

    def add_file_words(self, client_path: Path) -> None:
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
