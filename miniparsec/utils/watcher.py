import os
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from miniparsec import paths
from miniparsec.schemes import Scheme
from miniparsec.utils import console, file


class Watcher:
    def __init__(self, directory, handler):
        self.observer = Observer()
        self.handler = handler
        self.directory = directory

    def run(self):
        self.observer.schedule(self.handler, self.directory, recursive=True)
        self.observer.start()
        console.log(f"Watcher running in '{self.directory}'. You can add files now.")
        try:
            while True:
                time.sleep(1)
        except Exception as e:
            self.observer.stop()
            console.error(f"Watcher raised exception '{e}'.")
        self.observer.join()
        console.log("Watcher Terminated.")


class MyHandler(FileSystemEventHandler):
    def __init__(self, scheme: Scheme) -> None:
        self.scheme: Scheme = scheme

    def on_any_event(self, event):
        match event.event_type:
            case "created":
                client_path = Path(event.src_path)
                if event.is_directory:
                    server_path = file.get_server_path(client_path)
                    try:
                        os.mkdir(server_path)
                    except FileExistsError:
                        pass
                else:
                    basename = client_path.name
                    is_tempfile = basename[:5] == "temp_"
                    is_protected = basename in self.scheme.protected_filenames
                    if not is_protected and not is_tempfile:
                        console.log(f"File '{client_path}' added by user.")
                        try:
                            t1, t2 = self.scheme.add_file(client_path)
                        except UnicodeDecodeError:
                            console.error(f"Failed to decode file {client_path}")
                        else:
                            console.log(f"Encryption: {t1:.2f}s, indexing: {t2:.2f}s\n")

            case "deleted":
                file_path = event.src_path
                console.log(f"File '{file_path}' deleted by user.")
                _ = self.scheme.remove_file(file_path)