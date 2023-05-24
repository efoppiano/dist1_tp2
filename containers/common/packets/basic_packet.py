import struct
from abc import ABC
from dataclasses import dataclass
from typing import List, Union

import typing

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


class Array(List[T], typing.Generic[T, S]):
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

    def __search_union_index(self, element, union_types) -> int:
        element_is_list = isinstance(element, list)

        for i, union_type in enumerate(union_types):
            if element_is_list:
                if typing.get_origin(union_type) is list:
                    element_inner_type = type(element[0])
                    union_inner_type = typing.get_args(union_type)[0]
                    if element_inner_type == union_inner_type:
                        return i
            elif typing.get_origin(union_type) == type(element) or union_type == type(element):
                return i

        raise ValueError(f"Element {element} is not of any type in {union_types}")

    def __serialize_element(self, element, element_type) -> bytes:
        result_bytes = b""
        if element_type == bool:
            result_bytes += pack_bool(element)
        elif element_type == int:
            result_bytes += pack_int(element)
        elif element_type == float:
            result_bytes += pack_float(element)
        elif element_type == str:
            encoded_string = element.encode()
            encoded_string_length = pack_int(len(encoded_string))
            result_bytes += encoded_string_length + encoded_string
        elif element_type == bytes:
            encoded_bytes_length = pack_int(len(element))
            result_bytes += encoded_bytes_length + element
        elif typing.get_origin(element_type) is typing.Union:
            union_types = typing.get_args(element_type)
            idx = self.__search_union_index(element, union_types)
            result_bytes += pack_byte(idx)
            result_bytes += self.__serialize_element(element, union_types[idx])

        elif typing.get_origin(element_type) == Array:
            element_of_array_type = typing.get_args(element_type)[0]
            for e in element:
                result_bytes += self.__serialize_element(e, element_of_array_type)

        elif typing.get_origin(element_type) == list:
            list_length = len(element)
            encoded_list_length = pack_int(list_length)
            element_of_list_type = typing.get_args(element_type)[0]
            result_bytes += encoded_list_length
            for e in element:
                result_bytes += self.__serialize_element(e, element_of_list_type)
        elif element_type is type(None):
            pass
        else:
            try:
                result_bytes += element.encode()
            except Exception as e:
                raise Exception(f"Erorr while serializing element {element} of type {element_type}: {e}")

        return result_bytes

    @classmethod
    def __deserialize_element(cls, data, field_type):
        if field_type == bool:
            return unpack_bool(data[:1])[0], 1
        elif field_type == int:
            return unpack_int(data[:4])[0], 4
        elif field_type == float:
            return unpack_float(data[:8])[0], 8
        elif field_type == bytes:
            bytes_length, data = unpack_int(data[:4])[0], data[4:]
            element = data[:bytes_length]
            return element, 4 + bytes_length
        elif field_type == str:
            string_length, data = unpack_int(data[:4])[0], data[4:]
            element = data[:string_length].decode()
            return element, 4 + string_length
        elif field_type is type(None):
            return None, 0
        elif typing.get_origin(field_type) is Array:
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
        elif typing.get_origin(field_type) is list:
            list_length = unpack_int(data[:4])[0]
            data = data[4:]
            element_type = typing.get_args(field_type)[0]
            elements = []
            bytes_read = 4
            for _ in range(list_length):
                list_element, new_bytes_read = cls.__deserialize_element(data, element_type)
                data = data[new_bytes_read:]
                bytes_read += new_bytes_read
                elements.append(list_element)

            return elements, bytes_read
        elif typing.get_origin(field_type) is typing.Union:
            union_types = typing.get_args(field_type)
            union_type_pos = unpack_byte(data[:1])[0]
            data = data[1:]
            union_type = union_types[union_type_pos]
            element, new_bytes_read = cls.__deserialize_element(data, union_type)
            return element, new_bytes_read + 1

        else:
            try:
                element, new_bytes_read = field_type.__deserialize(data)
                return element, new_bytes_read

            except Exception as e:
                raise Exception(f"Erorr while deserializing element {element} of type {field_type}: {e}")

    @classmethod
    def __deserialize(cls, data: bytes) -> typing.Tuple["BasicPacket", int]:
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
