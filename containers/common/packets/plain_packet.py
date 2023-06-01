import dataclasses
from abc import ABC
from dataclasses import dataclass


@dataclass
class PlainPacket(ABC):
    def encode(self) -> bytes:
        fields = dataclasses.fields(self)
        values = [str(getattr(self, field.name)).encode() for field in fields]

        return b",".join(values)

    @classmethod
    def decode(cls, data: bytes) -> "PlainPacket":
        fields = dataclasses.fields(cls)
        values = data.split(b",")
        values_casted = [field.type(value) if field.type != str else value.decode() for (field, value) in
                         zip(fields, values)]

        return cls(*values_casted)
