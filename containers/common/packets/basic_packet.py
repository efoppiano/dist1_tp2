import pickle
import struct
from abc import ABC
from dataclasses import dataclass
import typing
from typing import List, Union, Generic, Type, Any, Tuple

T = typing.TypeVar("T")
S = typing.TypeVar("S")

unpack_byte = struct.Struct("B").unpack
unpack_bool = struct.Struct("?").unpack
unpack_int = struct.Struct("i").unpack
unpack_float = struct.Struct("d").unpack

pack_byte = struct.Struct("B").pack
pack_bool = struct.Struct("?").pack
pack_int = struct.Struct("i").pack
pack_float = struct.Struct("d").pack

Element = Union[int, float, str, bool, bytes, List[T], "BasicPacket", "Array[T, S]", None]


class Array(List[T], Generic[T, S]):
    size: S

    def __init__(self, *args, **kwargs):
        elements = args[0]
        if not issubclass(type(elements), list):
            raise TypeError("First argument must be a list")
        super().__init__(*args, **kwargs)


@dataclass
class BasicPacket(ABC):
    def encode(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def decode(cls, data: bytes) -> "BasicPacket":
        return pickle.loads(data)
