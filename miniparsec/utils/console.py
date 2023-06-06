from typing import Any

import rich
from rich.console import Console

console = Console()


# pylint: disable-next=redefined-builtin
def print(*args: Any, **kwargs: Any) -> None:
    """Affichage de texte."""
    return rich.print(*args, **kwargs)


def log(
    *messages: Any,
    verbose: int = True,
    end: str = "\n",
    mode: str = "",
    _stack_offset: int = 2,
) -> None:
    """Affichage de logs."""
    if not verbose:
        return

    messages_list = list(str(m) for m in messages)
    for i, message in enumerate(messages_list):
        if mode == "error":
            prefix = "[bold]Error:[/bold]"
            messages_list[i] = f"[red]{prefix} {message}[/]"
        elif mode == "warning":
            prefix = "[bold]Warning:[/bold]"
            messages_list[i] = f"[yellow]{prefix} {message}[/]"
        elif mode == "timing":
            prefix = "[bold]Timing:[/bold]"
            messages_list[i] = f"[blue]{prefix} {message}[/]"

    console.log(*messages_list, _stack_offset=_stack_offset, markup=True, end=end)


def error(*messages: Any, verbose: bool = True) -> None:
    """Affichage d'erreurs."""
    return log(*messages, mode="error", _stack_offset=3, verbose=verbose)


def warning(*messages: Any, verbose: bool = True) -> None:
    """Affichage d'avertissements."""
    return log(*messages, mode="warning", _stack_offset=3, verbose=verbose)


def timing(*messages: Any, verbose: bool = True) -> None:
    """Affichage de durées d'exécution."""
    return log(*messages, mode="timing", _stack_offset=3, verbose=verbose)
