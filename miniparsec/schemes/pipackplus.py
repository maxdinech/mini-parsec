from psycopg import Connection, sql

from .pibasplus import PiBasPlus


class PiPackPlus(PiBasPlus):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.B = 10
