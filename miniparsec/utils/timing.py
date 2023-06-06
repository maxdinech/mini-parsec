"""Décorateur obtenir les durées d'exécution de fonctions."""

from collections.abc import Callable
from functools import wraps
from time import time
from typing import Any

from . import console


def timing(function: Callable) -> Any:
    """Décorateur obtenir les durées d'exécution de fonctions."""

    @wraps(function)
    def wrap(*args: Any, **kwargs: Any) -> Any:
        time_start = time()
        result = function(*args, **kwargs)
        time_end = time()
        duration = time_end - time_start
        console.timing(
            f"function [bold]{function.__name__}[/bold] took {duration:.3f} seconds."
        )
        return result

    return wrap
