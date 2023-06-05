from dataclasses import dataclass


class Token:
    pass


@dataclass
class PiToken(Token):
    k1: bytes
    k2: bytes


@dataclass
class SophosToken(Token):
    k1: bytes
    k2: bytes


@dataclass
class DianaToken(Token):
    k1: bytes
    k2: bytes
