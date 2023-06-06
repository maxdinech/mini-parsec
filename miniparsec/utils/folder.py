import os
import shutil

from miniparsec.utils import console


def create(folder_path: str, verbose: bool = True) -> None:
    try:
        os.mkdir(folder_path)
    except FileExistsError:
        pass
    else:
        console.log(f"Folder '{folder_path}' created.", verbose=verbose)


def delete(folder_path: str, verbose: bool = True) -> None:
    try:
        shutil.rmtree(folder_path)
    except FileNotFoundError:
        pass
    else:
        console.log(f"Folder '{folder_path}' deleted.", verbose=verbose)


def empty(folder_path: str, verbose: bool = True) -> None:
    delete(folder_path, verbose=False)
    create(folder_path, verbose=False)
    if verbose:
        console.log(f"Folder '{folder_path}' emptied.", verbose=verbose)
