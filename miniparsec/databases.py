import psycopg
from psycopg import Connection, sql

from miniparsec.utils import console


def connect_db() -> Connection:
    conn = psycopg.connect(
        dbname="mini-parsec", host="localhost", user="admin", port="5432"
    )
    return conn


def drop_table(conn: Connection, table_name: str) -> None:
    cursor = conn.cursor()
    query = sql.SQL("DROP TABLE {}").format(sql.Identifier(table_name))
    try:
        cursor.execute(query)
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        console.warning(f"Tried to drop nonexistent table '{table_name}'.")
    else:
        console.log(f"Table '{table_name}' dropped.")
    conn.commit()


def create_table(conn: Connection, table_name: str, columns: dict[str, str]) -> None:
    cursor = conn.cursor()
    query = sql.SQL("CREATE TABLE {} ()").format(sql.Identifier(table_name))
    cursor.execute(query)
    for name, dtype in columns.items():
        query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
            sql.Identifier(table_name),
            sql.Identifier(name),
            sql.Identifier(dtype),
        )
        cursor.execute(query)
    conn.commit()


def truncate_table(conn: Connection, table_name: str) -> None:
    cursor = conn.cursor()
    query = sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
    try:
        cursor.execute(query)
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        console.warning(f"Tried to truncate nonexistent table '{table_name}'.")
    else:
        console.log(f"Table '{table_name}' truncated.")
    conn.commit()


def create_index(conn: Connection, table_name: str) -> None:
    cursor = conn.cursor()
    query = sql.SQL("CREATE INDEX {} ON {}(token);").format(
        sql.Identifier(f"idx_{table_name}"), sql.Identifier(table_name)
    )
    cursor.execute(query)
    conn.commit()
