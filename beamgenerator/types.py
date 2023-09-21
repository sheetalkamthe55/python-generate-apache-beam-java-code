from typing import ClassVar


class AbstractPCollectionType:
    name: ClassVar[str] = ""
    def __init__(self, name):
        self.name = name

class SimplePCollectionType(AbstractPCollectionType):
    pass

class InputPCollectionType(AbstractPCollectionType):
    fields: ClassVar[list[str, str]] = [] # list of tuples containing the field name and type

    def __init__(self, name: str, fields: list[str, str] = []):
        self.name = name
        self.fields = fields

    def add_field(self, field: tuple[str, str]):
        """tuple containing the field name and type"""
        self.fields.append(field)
