from pathlib import Path

from psycopg import Connection, sql
from rich.progress import Progress, track

from miniparsec import crypt, databases, index
from miniparsec.paths import CLIENT_ROOT
from miniparsec.tokens import PiToken
from miniparsec.utils import console

from .pibas import PiBas


class PiBasPlus(PiBas):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.tables_names = ("edb", "edb2")
        self.protected_filenames = {"edb_count.pkl", "edb2_count.pkl"}

    def reset(self):
        super().reset()

    def tokenize(self, word: str, prefix: str = "") -> PiToken:
        return PiToken(
            crypt.hmac(f"{prefix}1{word}", self.key),
            crypt.hmac(f"{prefix}2{word}", self.key),
        )

    def search_token(self, token: PiToken, table_name: str, max_count=None) -> set[str]:
        return super().search_token(token, table_name, max_count)

    def search_word(self, word: str) -> set[str]:
        results = set()
        for table_name in self.tables_names:
            token = self.tokenize(word, prefix=table_name)
            table_results = self.search_token(token, table_name)
            console.log(f"{len(table_results)} results in table {table_name}.")
            results.update(table_results)
        return results

    def add_word_helper(
        self, word: str, count: int, filename: str, table_name: str
    ) -> None:
        cursor = self.conn.cursor()
        token = self.tokenize(word, prefix=table_name)

        query = sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(
            sql.Identifier(table_name)
        )

        entry_key = crypt.hmac(str(count), key=token.k1)
        entry_value = crypt.encrypt(filename, key=token.k2)
        data = (entry_key, entry_value)

        cursor.execute(query, data)

    def add_word(self, word: str, client_path: Path) -> None:
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        count = edb2_count.get(word, 0)

        filename = str(client_path.relative_to(CLIENT_ROOT))
        self.add_word_helper(word, count, filename, "edb2")

        edb2_count[word] = count + 1
        crypt.encrypt_pickle(edb2_count, "edb2_count", self.key)

    def add_file_words(self, client_path: Path, verbose=True) -> int:
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        file_index, word_count = index.file_index(client_path)

        filename = str(client_path.relative_to(CLIENT_ROOT))
        for word in file_index:
            count = edb2_count.get(word, 0)
            self.add_word_helper(word, count, filename, "edb2")
            edb2_count[word] = count + 1
        self.conn.commit()
        console.log(
            f"Words: {word_count:6,d}, Unique words : {len(file_index):6,d}, edb2_count : {len(edb2_count):6,d}.",
            verbose=verbose,
        )
        crypt.encrypt_pickle(edb2_count, "edb2_count", self.key)

        return len(file_index)

    def merge(self, newkey: bytes | None = None) -> None:
        """Fusion de EDB et EDB2."""
        edb_count: dict[str, int] = crypt.decrypt_pickle("edb_count", self.key, {})
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        new_data = {}

        console.log("Merging tables...")

        with Progress() as progress:
            total = sum(edb2_count.values())
            load_words = progress.add_task("Loading words...", total=total)

            for word in edb2_count:
                edb2_token = self.tokenize(word, prefix="edb2")
                results = self.search_token(
                    edb2_token, "edb2", max_count=edb2_count[word]
                )
                new_data[word] = results
                progress.update(load_words, advance=edb2_count[word])

            total = sum(len(v) for v in new_data.values())
            add_words = progress.add_task("Adding words...", total=total)

            for word in new_data:
                count = edb_count.get(word, 0)
                for filename in new_data[word]:
                    self.add_word_helper(word, count, str(filename), "edb")
                    count += 1
                    edb_count[word] = count
                progress.update(add_words, advance=len(new_data[word]))
            self.conn.commit()

        databases.truncate_table(self.conn, "edb2")

        crypt.encrypt_pickle(edb_count, "edb_count", self.key)
        crypt.encrypt_pickle(edb2_count, "edb2_count", self.key)
