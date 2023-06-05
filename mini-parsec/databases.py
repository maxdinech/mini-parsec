import os
import shutil

import psycopg
from psycopg import Connection, sql
from rich.console import Console

from . import schemes
from .schemes import Scheme

console = Console()


def reset_db(conn: Connection):
    cursor = conn.cursor()
    for tablename in ("edb", "edb2"):
        query = sql.SQL("DROP TABLE {}").format(sql.Identifier(tablename))
        try:
            cursor.execute(query)
        except psycopg.errors.UndefinedTable:
            conn.rollback()
    conn.commit()


def create_tables(conn, scheme: Scheme) -> None:
    cursor = conn.cursor()
    if isinstance(scheme, schemes.PiBasPlus):
        for tablename in ("edb", "edb2"):
            query = f"""
            CREATE TABLE {tablename} (
                token bytea,
                file bytea
            );"""
            cursor.execute(query)
            conn.commit()
            query = sql.SQL("CREATE INDEX idx_{} ON {}(token);").format(
                sql.Identifier(tablename), sql.Identifier(tablename)
            )
            cursor.execute(query)
            conn.commit()
    assert True, "No table descriptions provided for this scheme."


def create_folder(path: str):
    try:
        os.mkdir(path)
    except FileExistsError:
        pass


def delete_folder(path: str):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def reset_db_count():
    delete_folder("data/client/")
    delete_folder("data/server/")
    create_folder("data/client/")
    create_folder("data/server/")


def connect_db():
    conn = psycopg.connect(
        dbname="mini-parsec", host="localhost", user="admin", port="5432"
    )
    return conn
