import pickle

import nacl.secret
import nacl.utils
from nacl.hash import blake2b

from miniparsec.utils import console


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


def encrypt_file(path: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    dest_path = "data/server/" + path.split("client/")[-1]
    try:
        with open(path, "rb") as f:
            encrypted = box.encrypt(f.read())
            with open(dest_path, "wb") as ef:
                ef.write(encrypted)
    except FileNotFoundError:
        console.log("File to encrypt not found.")


def decrypt_file(path: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    dest_path = "data/client/" + path.split("server/")[-1]
    try:
        with open(path, "rb") as f:
            decrypted = box.decrypt(f.read())
            with open(dest_path, "wb") as ef:
                ef.write(decrypted)
    except FileNotFoundError:
        console.log("File to decrypt not found.")


def encrypt_dict_file(dictionnary: dict, filename: str, key: bytes) -> None:
    box = nacl.secret.SecretBox(key)
    with open(f"data/client/{filename}.pkl", "wb") as f:
        pickle.dump(dictionnary, f)
    with open(f"data/client/{filename}.pkl", "rb") as f:
        dictionnary_file = box.encrypt(f.read())
        with open(f"data/server/{filename}", "wb") as ef:
            ef.write(dictionnary_file)


def decrypt_dict_file(filename: str, key: bytes) -> dict:
    box = nacl.secret.SecretBox(key)
    try:
        with open(f"data/server/{filename}", "rb") as ef:
            dictionnary_file = box.decrypt(ef.read())
            with open(f"data/client/{filename}.pkl", "wb") as f:
                f.write(dictionnary_file)
    except FileNotFoundError:
        console.error("Dict file not found.")

    try:
        with open("data/client/db_count.pkl", "rb") as f:
            dictionnary = pickle.load(f)
    except FileNotFoundError:
        dictionnary = {}
    return dictionnary
