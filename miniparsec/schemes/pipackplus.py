import itertools

from psycopg import Connection, sql
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
        edb_count: dict[str, int] = crypt.decrypt_pickle("edb_count", self.key, {})
        edb2_count: dict[str, int] = crypt.decrypt_pickle("edb2_count", self.key, {})
        new_data = {}

        console.log("Merging tables...")

        with Progress() as progress:
            total = sum(edb2_count.values())
            load_words = progress.add_task("Loading EDB words...", total=total)

            for word in edb_count:
                edb_token = self.tokenize(word, prefix="edb")
                last_count = edb_count[word] - 1
                results = self.search_token(edb_token, "edb", count=last_count)
                new_data[word] = results
                edb_count[word] -= 1
                progress.update(load_words, advance=edb2_count[word])

            total = sum(edb2_count.values())
            load_words2 = progress.add_task("Loading EDB2 words...", total=total)

            for word in edb2_count:
                edb2_token = self.tokenize(word, prefix="edb2")
                max_count = edb2_count[word]
                results = self.search_token(edb2_token, "edb2", max_count=max_count)
                if word not in new_data:
                    new_data[word] = set()
                new_data.get(word, set()).update(results)
                progress.update(load_words2, advance=edb2_count[word])

            total = sum(1 + (len(v) // self.B) for v in new_data.values())
            add_words = progress.add_task("Adding words to EDB...", total=total)

            for word in new_data:
                count = edb_count.get(word, 0)
                filenames = list(new_data[word]) + [None] * self.B  # Pour itérer
                for filename_group in zip(*[iter(filenames)] * self.B):
                    if all(f is None for f in filename_group):
                        continue
                    filename_str = str(set(f for f in filename_group if f is not None))
                    self.add_word_helper(word, count, str(filename_str), "edb")
                    count += 1
                    edb_count[word] = count
                progress.update(add_words, advance=len(new_data[word]))
            self.conn.commit()

        databases.truncate_table(self.conn, "edb2")

        crypt.encrypt_pickle(edb_count, "edb_count", self.key)
        crypt.encrypt_pickle(edb2_count, "edb2_count", self.key)
