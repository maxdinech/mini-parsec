from psycopg import Connection, sql

from miniparsec import crypt, databases, index
from miniparsec.tokens import PiToken
from miniparsec.utils import console

from .scheme import Scheme


class PiBas(Scheme):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.tables_names: set[str] = {"edb"}

    def reset(self):
        super().reset()
        for table_name in self.tables_names:
            databases.drop_table(self.conn, table_name)
            databases.create_table(
                self.conn, table_name, {"token": "bytea", "file": "bytea"}
            )
            databases.create_index(self.conn, table_name)

    def tokenize(self, word: str, prefix: str = "", key: bytes = b"") -> PiToken:
        if not key:
            key = self.key
        return PiToken(
            crypt.hmac(f"{prefix}1{word}", key),
            crypt.hmac(f"{prefix}2{word}", key),
        )

    def search_token(
        self,
        token: PiToken,
        table_name: str,
        count: int = 0,
        max_count: int | None = None,
    ) -> set[str]:
        cursor = self.conn.cursor()
        result = set()

        # Pour une seule query
        if count:
            max_count = count + 1

        # Tant au'on ne dépasse pas `max_count` ou que Postgres ne renvoie pas None
        while max_count is None or count < max_count:
            query = sql.SQL("SELECT file FROM {} WHERE token = %s;").format(
                sql.Identifier(table_name)
            )
            query_key = crypt.hmac(str(count), token.k1)
            data = (query_key,)
            cursor.execute(query, data)
            fetchone = cursor.fetchone()
            if fetchone is None:
                break
            query_result = crypt.decrypt(fetchone[0], key=token.k2).decode("utf-8")
            result_set: set = eval(query_result)

            for entry in result_set:
                file = entry
                result.add(file)

            count += 1
        return result

    def search_word(self, word: str) -> set[str]:
        results = set()
        word = index.stem(word)
        for table_name in self.tables_names:
            token = self.tokenize(word, prefix=table_name)
            table_results = self.search_token(token, table_name)
            console.log(f"{len(table_results)} results in table {table_name}.")
            results.update(table_results)

        return results
