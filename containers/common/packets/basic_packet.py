import struct
from abc import ABC
from dataclasses import dataclass
import typing
from typing import List, Union, Generic, Type, Tuple, Any

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
        class_fields = self.__class__.__dict__["__dataclass_fields__"]
        elements = [getattr(self, field_name) for field_name in class_fields]
        result_bytes = b""

        for (element, field_name) in zip(elements, class_fields):
            field_type = class_fields[field_name].type
            result_bytes += self.__serialize_element(element, field_type)

        return result_bytes

    @staticmethod
    def __get_type_str(element: Element) -> str:
        if isinstance(element, int):
            return "int"
        elif isinstance(element, float):
            return "float"
        elif isinstance(element, str):
            return "str"
        elif isinstance(element, bool):
            return "bool"
        elif isinstance(element, bytes):
            return "bytes"
        elif isinstance(element, Array):
            inner_type = BasicPacket.__get_type_str(element[0])
            array_size = len(element)
            return f"Array[{inner_type}, {array_size}]"
        elif isinstance(element, list):
            inner_type = BasicPacket.__get_type_str(element[0])
            return f"List[{inner_type}]"
        elif isinstance(element, BasicPacket):
            return element.__class__.__name__
        elif element is None:
            return "NoneType"
        else:
            raise ValueError(f"Element {element} has no type")

    @staticmethod
    def __get_type_str_from_typing(element_type: Type) -> str:
        if element_type == int:
            return "int"
        elif element_type == float:
            return "float"
        elif element_type == str:
            return "str"
        elif element_type == bool:
            return "bool"
        elif element_type == bytes:
            return "bytes"
        elif typing.get_origin(element_type) == list:
            inner_type = BasicPacket.__get_type_str_from_typing(typing.get_args(element_type)[0])
            return f"List[{inner_type}]"
        elif typing.get_origin(element_type) == Array:
            inner_type = BasicPacket.__get_type_str_from_typing(typing.get_args(element_type)[0])
            array_size = typing.get_args(element_type)[1]
            return f"Array[{inner_type}, {array_size}]"
        elif issubclass(element_type, BasicPacket):
            return element_type.__name__
        elif element_type is type(None):
            return "NoneType"
        else:
            raise ValueError(f"Element type {element_type} has no type")

    def __search_union_index(self, element: Element, union_types: Tuple[Any]) -> int:
        if len(union_types) > 256:
            raise ValueError(f"Union type {union_types} has more than 256 types")
        element_type_str = self.__get_type_str(element)
        for idx, union_type in enumerate(union_types):
            if self.__get_type_str_from_typing(union_type) == element_type_str:
                return idx

        raise ValueError(f"Element {element} is not of any type in {union_types}")

    def __serialize_element(self, element: Element, element_type: Type) -> bytes:
        if element_type == bool:
            return pack_bool(element)
        elif element_type == int:
            return pack_int(element)
        elif element_type == float:
            return pack_float(element)
        elif element_type == str:
            encoded_string = element.encode()
            encoded_string_length = pack_int(len(encoded_string))
            return encoded_string_length + encoded_string
        elif element_type == bytes:
            encoded_bytes_length = pack_int(len(element))
            return encoded_bytes_length + element
        elif typing.get_origin(element_type) is Union:
            union_types = typing.get_args(element_type)
            idx = self.__search_union_index(element, union_types)
            return pack_byte(idx) + self.__serialize_element(element, union_types[idx])
        elif typing.get_origin(element_type) == Array:
            if type(element) != Array:
                raise TypeError(f"Element {element} is not of type Array")

            element_of_array_type = typing.get_args(element_type)[0]
            return b"".join([self.__serialize_element(e, element_of_array_type) for e in element])
        elif typing.get_origin(element_type) == list:
            list_length = len(element)
            encoded_list_length = pack_int(list_length)
            element_of_list_type = typing.get_args(element_type)[0]
            return encoded_list_length + b"".join([self.__serialize_element(e, element_of_list_type) for e in element])
        elif element_type is type(None):
            return b""
        else:
            try:
                return element.encode()
            except Exception as e:
                raise ValueError(f"Element {element} has no known type")

    @classmethod
    def __deserialize_array(cls, data: bytes, field_type: Type) -> Tuple[List[Element], int]:
        array_length = typing.get_args(field_type)[1]
        element_type = typing.get_args(field_type)[0]
        elements = []
        bytes_read = 0
        for _ in range(array_length):
            element, new_bytes_read = cls.__deserialize_element(data, element_type)
            data = data[new_bytes_read:]
            bytes_read += new_bytes_read
            elements.append(element)

        return elements, bytes_read

    @classmethod
    def __deserialize_bytes(cls, data: bytes) -> Tuple[bytes, int]:
        bytes_length, data = unpack_int(data[:4])[0], data[4:]
        element = data[:bytes_length]
        return element, 4 + bytes_length

    @classmethod
    def __deserialize_string(cls, data: bytes) -> Tuple[str, int]:
        string_length, data = unpack_int(data[:4])[0], data[4:]
        element = data[:string_length].decode()
        return element, 4 + string_length

    @classmethod
    def __deserialize_list(cls, data: bytes, inner_field_type: Type) -> Tuple[List[Element], int]:
        list_length = unpack_int(data[:4])[0]
        data = data[4:]
        elements = []
        bytes_read = 4
        for _ in range(list_length):
            list_element, new_bytes_read = cls.__deserialize_element(data, inner_field_type)
            data = data[new_bytes_read:]
            bytes_read += new_bytes_read
            elements.append(list_element)

        return elements, bytes_read

    @classmethod
    def __deserialize_union(cls, data: bytes, union_types: Tuple[Any]) -> Tuple[Element, int]:
        union_type_pos = unpack_byte(data[:1])[0]
        data = data[1:]
        union_type = union_types[union_type_pos]
        element, new_bytes_read = cls.__deserialize_element(data, union_type)
        return element, new_bytes_read + 1

    @classmethod
    def __deserialize_element(cls, data: bytes, field_type: Type) -> Tuple[Element, int]:
        if field_type == bool:
            return unpack_bool(data[:1])[0], 1
        elif field_type == int:
            return unpack_int(data[:4])[0], 4
        elif field_type == float:
            return unpack_float(data[:8])[0], 8
        elif field_type == bytes:
            return cls.__deserialize_bytes(data)
        elif field_type == str:
            return cls.__deserialize_string(data)
        elif field_type is type(None):
            return None, 0
        elif typing.get_origin(field_type) is Array:
            return cls.__deserialize_array(data, field_type)
        elif typing.get_origin(field_type) is list:
            return cls.__deserialize_list(data, typing.get_args(field_type)[0])
        elif typing.get_origin(field_type) is Union:
            return cls.__deserialize_union(data, typing.get_args(field_type))
        else:
            try:
                element, new_bytes_read = field_type.__deserialize(data)
                return element, new_bytes_read

            except Exception as e:
                raise ValueError(f"Element {data} has no known type") from e

    @classmethod
    def __deserialize(cls, data: bytes) -> ["BasicPacket", int]:
        class_fields = cls.__dict__["__dataclass_fields__"]
        elements = []
        bytes_read = 0
        for field_name in class_fields:
            field_type = class_fields[field_name].type
            element, new_bytes_read = cls.__deserialize_element(data, field_type)
            data = data[new_bytes_read:]
            bytes_read += new_bytes_read

            elements.append(element)

        return cls(*elements), bytes_read

    @classmethod
    def decode(cls, data: bytes) -> "BasicPacket":
        deserialized, bytes_read = cls.__deserialize(data)
        if bytes_read != len(data):
            raise ValueError(f"Not all bytes were read ({bytes_read} out of {len(data)})")

        return deserialized
