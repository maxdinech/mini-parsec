import os
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from miniparsec.schemes import Scheme
from miniparsec.utils import console


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
        if event.event_type == "created":
            path = event.src_path
            if event.is_directory:
                server_path = "data/server/" + path.split("client/")[-1]
                try:
                    os.mkdir(server_path)
                except FileExistsError:
                    pass
            else:
                filename = path.split("/")[-1]
                if filename not in self.scheme.protected_filenames:
                    console.log(f"Adding file '{path}'")
                    try:
                        t1, t2 = self.scheme.add_file(path)
                    except UnicodeDecodeError:
                        console.log(f"Error: Failed to decode file {path}")
                    else:
                        console.log(f"Encryption: {t1:.2f}s, indexing: {t2:.2f}s\n")
