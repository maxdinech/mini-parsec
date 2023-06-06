import os
from pathlib import Path

from miniparsec.paths import CLIENT_ROOT, SERVER_ROOT

from . import console


def delete(file_path: Path, verbose: bool = True):
    try:
        os.remove(file_path)
    except FileNotFoundError:
        console.error(f"File to delete '{file_path}' not found.")
    else:
        console.log(f"File '{file_path}' deleted.", verbose=verbose)


def get_client_path(server_path: Path) -> Path:
    return CLIENT_ROOT / server_path.relative_to(SERVER_ROOT)


def get_server_path(client_path: Path) -> Path:
    return SERVER_ROOT / client_path.relative_to(CLIENT_ROOT)


def rename(file_path: Path, basename: str) -> Path:
    return file_path.parent / basename
