from psycopg import Connection
from rich.progress import Progress

from miniparsec import crypt, databases
from miniparsec.utils import console

from .pibasplus import PiBasPlus


class PiPackPlus(PiBasPlus):
    def __init__(self, key: bytes, conn: Connection, B: int) -> None:
        super().__init__(key, conn)
        self.B = B

    def merge(self) -> None:
        """Fusion de EDB et EDB2."""

        B = self.B
        recrypt = self.newkey is not None

        edb_count: dict[str, int] = crypt.decrypt_pickle("edb_count", self.key, {})
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        global_index = {}

        console.log("Merging tables...")

        with Progress() as progress:
            total = sum(edb2_count.values())
            load_words = progress.add_task("Loading EDB words...", total=total)

            for word in edb_count:
                edb_token = self.tokenize(word, prefix="edb")
                if recrypt:
                    max_count = edb_count[word]
                    results = self.search_token(edb_token, "edb", max_count=max_count)
                    edb_count[word] = 0
                else:
                    last_count = edb_count[word] - 1
                    results = self.search_token(edb_token, "edb", count=last_count)
                    edb_count[word] -= 1
                global_index[word] = results
                progress.update(load_words, advance=edb_count[word])

            total = sum(edb2_count.values())
            load_words2 = progress.add_task("Loading EDB2 words...", total=total)

            for word in edb2_count:
                edb2_token = self.tokenize(word, prefix="edb2")
                max_count = edb2_count[word]
                results = self.search_token(edb2_token, "edb2", max_count=max_count)
                if word not in global_index:
                    global_index[word] = set()
                global_index.get(word, set()).update(results)
                progress.update(load_words2, advance=edb2_count[word])

        databases.truncate_table(self.conn, "edb2")
        if recrypt:
            databases.truncate_table(self.conn, "edb")

        with Progress() as progress:
            total = sum(1 + (len(v) // B) for v in global_index.values())
            add_words = progress.add_task("Adding words to EDB...", total=total)

            for word in global_index:
                count = edb_count.get(word, 0)
                filenames = list(global_index[word]) + [None] * B  # Pour itérer
                for filename_group in zip(*[iter(filenames)] * B):
                    if all(f is None for f in filename_group):
                        continue
                    filename_str = str(set(f for f in filename_group if f is not None))
                    self.add_word_helper(word, count, str(filename_str), "edb")
                    count += 1
                    edb_count[word] = count
                progress.update(add_words, advance=len(global_index[word]))
            self.conn.commit()

        key = self.key if self.newkey is None else self.newkey
        crypt.encrypt_pickle(edb_count, "edb_count", key)
        crypt.encrypt_pickle(edb2_count, "edb2_count", key)
