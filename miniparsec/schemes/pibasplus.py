from pathlib import Path

from psycopg import Connection, sql
from rich.progress import Progress

from miniparsec import crypt, databases, index
from miniparsec.paths import CLIENT_ROOT, SERVER_ROOT
from miniparsec.utils import console, file, timing

from .pibas import PiBas


class PiBasPlus(PiBas):
    def __init__(self, key: bytes, conn: Connection) -> None:
        super().__init__(key, conn)
        self.tables_names: set[str] = {"edb", "edb2"}
        self.protected_filenames = {"edb_count.pkl", "edb2_count.pkl"}

    def reset(self):
        super().reset()

    def add_word_helper(
        self, word: str, count: int, filename: str, table_name: str
    ) -> None:
        cursor = self.conn.cursor()

        key = self.key if self.newkey is None else self.newkey
        token = self.tokenize(word, prefix=table_name, key=key)

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

        file_index: set[str]
        file_index = index.index_file(client_path)
        word_count = len(file_index)
        index_length = len(file_index)
        path = str(client_path.relative_to(CLIENT_ROOT))
        path_set = set((path,))
        for word in file_index:
            count = edb2_count.get(word, 0)
            self.add_word_helper(word, count, str(path_set), "edb2")
            edb2_count[word] = count + 1

        self.conn.commit()
        console.log(
            f"Words: {word_count:6,d}, Unique words : {index_length:6,d}, edb2_count : {len(edb2_count):6,d}.",
            verbose=verbose,
        )
        crypt.encrypt_pickle(edb2_count, "edb2_count", self.key)

        return index_length

    def merge(self) -> None:
        """Fusion de EDB et EDB2."""

        recrypt = self.newkey is not None

        edb_count: dict[str, int] = crypt.decrypt_pickle("edb_count", self.key, {})
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        global_index: dict[str, set[str]] = {}

        console.log("Merging tables...")

        if recrypt:
            with Progress() as progress:
                total = sum(edb2_count.values())
                load_words = progress.add_task("Loading EDB words...", total=total)

                for word in edb_count:
                    edb_token = self.tokenize(word, prefix="edb")
                    max_count = edb_count[word]
                    results = self.search_token(edb_token, "edb", max_count=max_count)
                    edb_count[word] = 0
                    global_index[word] = results
                    progress.update(load_words, advance=edb_count[word])

        with Progress() as progress:
            total = sum(edb2_count.values())
            load_words2 = progress.add_task("Loading EDB2 words...", total=total)

            for word in edb2_count:
                edb2_token = self.tokenize(word, prefix="edb2")
                results = self.search_token(
                    edb2_token, "edb2", max_count=edb2_count[word]
                )
                if word not in global_index:
                    global_index[word] = set()

                global_index.get(word, set()).update(results)
                progress.update(load_words2, advance=edb2_count[word])

        databases.truncate_table(self.conn, "edb2")
        if recrypt:
            databases.truncate_table(self.conn, "edb")

        with Progress() as progress:
            total = sum(len(v) for v in global_index.values())
            add_words = progress.add_task("Adding words to EDB...", total=total)

            for word in global_index:
                count = edb_count.get(word, 0)
                for entry in global_index[word]:
                    str_entry = str(set((entry,)))
                    self.add_word_helper(word, count, str_entry, "edb")
                    count += 1
                    edb_count[word] = count
                progress.update(add_words, advance=len(global_index[word]))
            self.conn.commit()

        databases.truncate_table(self.conn, "edb2")

        crypt.encrypt_pickle(edb_count, "edb_count", self.key)
        file.delete(SERVER_ROOT / "edb2_count")
        file.delete(CLIENT_ROOT / "edb2_count.pkl")
