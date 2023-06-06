import nacl.secret
import nacl.utils
from nacl.hash import blake2b
from psycopg import Connection, sql

from miniparsec import crypt, databases
from miniparsec.tokens import PiToken

from .scheme import Scheme


class PiBas(Scheme):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)

    def reset(self):
        table_names = ("edb", "edb2")
        for table_name in table_names:
            databases.drop_table(self.conn, table_name)
            databases.create_table(
                self.conn, table_name, {"token": "bytea", "file": "bytea"}
            )

    def tokenize(self, word: str) -> PiToken:
        return PiToken(
            crypt.hmac(f"1{word}", self.key),
            crypt.hmac(f"2{word}", self.key),
        )

    def search_token(self, token: PiToken) -> set[str]:
        cursor = self.conn.cursor()
        result = set()
        for table_name in ("edb", "edb2"):
            local_key = blake2b(token.k2)[:32]
            local_box = nacl.secret.SecretBox(local_key)
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
                path = local_box.decrypt(fetchone[0]).decode("utf-8")
                result.add(path)
                count += 1
        return result
