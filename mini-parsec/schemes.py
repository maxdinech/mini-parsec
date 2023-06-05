from dataclasses import dataclass


class Scheme:
    pass


class PiBasPlus(Scheme):
    def __str__(self) -> str:
        return "Pi_bas^+"


@dataclass
class PiPackPlus(Scheme):
    B: int

    def __str__(self) -> str:
        return "Pi_pack^+"
