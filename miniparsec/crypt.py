import pickle
from pathlib import Path
from typing import Any

import nacl.secret
import nacl.utils
from nacl.hash import blake2b

from miniparsec.paths import CLIENT_ROOT, SERVER_ROOT
from miniparsec.utils import console, file


def hmac(content: str | bytes, key: bytes = b"") -> bytes:
    if isinstance(content, str):
        content = content.encode("utf-8")
    return blake2b(content, key=key)[:32]


def encrypt(content: str | bytes, key: bytes) -> bytes:
    box = nacl.secret.SecretBox(key)
    if isinstance(content, str):
        content = content.encode("utf-8")
    return box.encrypt(content)


def decrypt(content: bytes, key: bytes) -> bytes:
    box = nacl.secret.SecretBox(key)
    return box.decrypt(content)


def encrypt_file(client_path: Path, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    server_path = file.get_server_path(client_path)
    try:
        with open(client_path, "rb") as f:
            encrypted = box.encrypt(f.read())
            with open(server_path, "wb") as ef:
                ef.write(encrypted)
    except FileNotFoundError:
        console.log("File to encrypt not found.")


def decrypt_file(server_path: Path, key: bytes, basename: str = "") -> None:
    box = nacl.secret.SecretBox(key)
    client_path = file.get_client_path(server_path)
    if basename:
        client_path = file.rename(client_path, basename)
    try:
        with open(server_path, "rb") as f:
            decrypted = box.decrypt(f.read())
            try:
                with open(client_path, "wb") as ef:
                    ef.write(decrypted)
            except Exception as e:
                console.error(e)
    except FileNotFoundError:
        console.log("File to decrypt not found.")


def encrypt_pickle(pickle_file: Any, filename: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    server_path = SERVER_ROOT / filename
    client_path = CLIENT_ROOT / f"{filename}.pkl"

    # Dump pickle
    with open(client_path, "wb") as f:
        pickle.dump(pickle_file, f)

    # Encrypt and upload file
    with open(client_path, "rb") as f:
        pickle_file = box.encrypt(f.read())
        with open(server_path, "wb") as ef:
            ef.write(pickle_file)


def decrypt_pickle(filename: str, key: bytes, defaultvalue: Any) -> Any:
    box = nacl.secret.SecretBox(key)
    server_path = SERVER_ROOT / filename
    client_path = CLIENT_ROOT / f"{filename}.pkl"

    # Download and decrypt file
    try:
        with open(server_path, "rb") as ef:
            pickle_file = box.decrypt(ef.read())
            with open(client_path, "wb") as f:
                f.write(pickle_file)
    except FileNotFoundError:
        console.error("Pickle file not found.")

    # Read pickle
    try:
        with open(client_path, "rb") as f:
            pickle_file = pickle.load(f)
    except FileNotFoundError:
        pickle_file = defaultvalue
    return pickle_file
