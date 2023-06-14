import argparse

from miniparsec import databases, schemes
from miniparsec.paths import CLIENT_ROOT
from miniparsec.utils import console, datasets, timing, watcher

from .crypt import hmac


def main() -> None:
    store = "store_true"
    parser = argparse.ArgumentParser(
        prog="Mini-Parsec",
        description="Mini-Parsec : serveur et recherche.",
    )
    subparsers = parser.add_subparsers(dest="command")

    dataset = subparsers.add_parser("dataset", help="Download datasets")
    dataset.add_argument("--all", help="Download all datasets", action="store_true")
    dataset.add_argument("-D", "--download", type=str, help="Dataset to download")

    search = subparsers.add_parser("search", help="Search words")
    search.add_argument("-K", "--key", type=str, help="search term", required=True)
    search.add_argument("-q", "--query", type=str, help="search term", required=True)
    search.add_argument("-i", "--inter", help="search intersection", action=store)
    search.add_argument("-u", "--union", help="search union", action=store)
    search.add_argument("-s", "--show", help="show results", action=store)

    server = subparsers.add_parser("server", help="Mini-parsec server")
    server.add_argument("-K", "--key", type=str, help="search term", required=True)
    server.add_argument("-r", "--reset", help="reset server", action=store)

    merge = subparsers.add_parser("merge", help="merge or re-encrypt.")
    merge.add_argument("-K", "--key", type=str, help="search term", required=True)
    merge.add_argument("-K2", "--newkey", type=str, help="new key", default=None)

    subparsers.required = True
    args = parser.parse_args()

    if args.command == "dataset":
        datasets.download_gutenberg_database()
        datasets.download_enron_database()
        return

    keyword: bytes = bytes(args.key, "utf-8")
    key: bytes = hmac(keyword)[:32]

    conn = databases.connect_db()

    SCHEME = schemes.PiPackPlus(key, conn, 100)

    match args.command:
        case "server":
            conn = databases.connect_db()
            if args.reset:
                console.log("Clearing databases and local files...")
                console.log("Creating databases...")
                SCHEME.reset()

            w = watcher.Watcher(CLIENT_ROOT, watcher.MyHandler(SCHEME))
            w.run()

        case "merge":
            conn = databases.connect_db()
            # Pas de nouvelle clé
            if args.newkey is None:
                _ = timing.timing(SCHEME.merge)()
            else:
                new_keyword: bytes = bytes(args.newkey, "utf-8")
                new_key: bytes = hmac(new_keyword)[:32]
                SCHEME.newkey = new_key
                _ = timing.timing(SCHEME.merge)()
                SCHEME.key = new_key
                SCHEME.newkey = None

            console.log("merge done.")

        case "search":
            conn = databases.connect_db()
            query = args.query
            words = query.split("+")
            words = [word.lower() for word in words]
            if len(words) == 1:
                word = words[0]
                _, results = timing.timing(SCHEME.search_word)(word)
            elif len(words) > 1:
                if args.union:
                    _, results = timing.timing(SCHEME.search_union)(words)
                else:
                    _, results = timing.timing(SCHEME.search_intersection)(words)
            else:
                console.error("No words provided.")
                results = set()
            if args.show:
                console.log(results)
            console.log(f"result: {len(results)} matches.")


if __name__ == "__main__":
    main()
