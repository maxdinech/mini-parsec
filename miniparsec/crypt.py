import os
import pickle
from pathlib import Path

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


def encrypt_dict_file(dictionnary: dict, filename: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    server_path = SERVER_ROOT / filename
    client_path = CLIENT_ROOT / f"{filename}.pkl"

    # Dump pickle
    with open(client_path, "wb") as f:
        pickle.dump(dictionnary, f)

    # Encrypt and upload file
    with open(client_path, "rb") as f:
        dictionnary_file = box.encrypt(f.read())
        with open(server_path, "wb") as ef:
            ef.write(dictionnary_file)


def decrypt_dict_file(filename: str, key: bytes) -> dict:
    box = nacl.secret.SecretBox(key)
    server_path = SERVER_ROOT / filename
    client_path = CLIENT_ROOT / f"{filename}.pkl"

    # Download and decrypt file
    try:
        with open(server_path, "rb") as ef:
            dictionnary_file = box.decrypt(ef.read())
            with open(client_path, "wb") as f:
                f.write(dictionnary_file)
    except FileNotFoundError:
        console.error("Dict file not found.")

    # Read pickle
    try:
        with open(client_path, "rb") as f:
            dictionnary = pickle.load(f)
    except FileNotFoundError:
        dictionnary = {}
    return dictionnary
