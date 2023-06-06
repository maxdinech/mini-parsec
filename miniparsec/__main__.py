import argparse

from nacl.hash import blake2b

from miniparsec import databases, schemes
from miniparsec.utils import console, datasets, watcher

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Mini-Parsec",
        description="Mini-Parsec : client et recherche.",
    )
    store = "store_true"
    _ = parser.add_argument("mode", nargs="?", default="sync")
    _ = parser.add_argument("-K", "--key", type=str, help="search term", required=True)
    _ = parser.add_argument("-r-", "--reset", help="reset server", action=store)
    _ = parser.add_argument("-s", "--show", help="show results", action=store)
    _ = parser.add_argument("-q", "--query", type=str, help="search term", default="")
    _ = parser.add_argument("-i", "--inter", help="query intersect", action=store)
    _ = parser.add_argument("-u", "--union", help="query union", action=store)
    _ = parser.add_argument("-K2", "--newkey", type=str, help="new key", default="")

    args = parser.parse_args()

    keyword: bytes = bytes(args.key, "utf-8")
    key: bytes = blake2b(keyword)[:32]

    conn = databases.connect_db()

    scheme = schemes.PiBasPlus(key, conn)

    match args.mode:
        case "dataset":
            datasets.download_gutenberg_database()
            datasets.download_enron_database()

        case "server":
            conn = databases.connect_db()
            if args.reset:
                console.log("Clearing databases and local files...")
                console.log("Creating databases...")
                scheme.reset()

            w = watcher.Watcher("data/client/", watcher.MyHandler(scheme))
            w.run()

        case "repack":
            conn = databases.connect_db()
            new_keyword = bytes(args.newkey, "utf-8")
            if new_keyword != keyword:
                new_key: bytes = blake2b(keyword)[:32]
                # Stuff here
                keyword, key = new_keyword, new_key

            console.log("Repack done.")

        case "search":
            conn = databases.connect_db()
            query = args.query
            words = query.split("+")
            words = [word.lower() for word in words]
            if len(words) == 1:
                results = scheme.search_word(words[0])
            elif len(words) > 1:
                if args.union:
                    results = scheme.search_union(words)
                else:
                    results = scheme.search_intersection(words)
            else:
                console.error("No words provided.")
                results = set()
            if args.show:
                console.log(results)
            console.log(f"result: {len(results)} matches.")

        case _:
            console.log("Invalid mode.")
