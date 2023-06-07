from psycopg import Connection, sql

from miniparsec import crypt, databases
from miniparsec.tokens import PiToken

from .scheme import Scheme


class PiBas(Scheme):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.tables_names = ("edb",)

    def reset(self):
        super().reset()
        for table_name in self.tables_names:
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

            path = crypt.decrypt(fetchone[0], key=token.k2).decode("utf-8")
            try:
                paths = eval(path)
            except Exception:
                result.add(path)
            else:
                if isinstance(paths, set):
                    result.update(paths)
                else:
                    result.add(path)
            count += 1
        return result
