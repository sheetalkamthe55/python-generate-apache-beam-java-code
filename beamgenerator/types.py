JAVA_NUMERIC_TYPE = ['Double', 'Integer', 'Float', 'Long', 'Short']

class AbstractPCollectionType:
    name: str = ""
    type: str = ""
    def __init__(self, type, name):
        self.type = type
        self.name = name # name of PCollection in Java code
        
class SimplePCollectionType(AbstractPCollectionType):
    def __eq__(self, other):
        if isinstance(other, SimplePCollectionType):
            return self.type == other.type
        else:
            return False
    
    def __ne__(self, __value: object) -> bool:
        return not self.__eq__(__value)
    
    def __str__(self):
        return self.type

class InputPCollectionType(AbstractPCollectionType):
    fields: list[str, str] = {} # list of tuples containing the field name and type

    def __init__(self, type: str, name: str, fields: dict[str, str] = {}):
        self.type = type
        self.name = name
        self.fields = fields

    def add_field(self, field: tuple[str, str]):
        """tuple containing the field name and type"""
        self.fields[field[0]] = field[1]

    def __eq__(self, other):
        if isinstance(other, InputPCollectionType):
            for field in self.fields:
                if field not in other.fields:
                    return False
                else: 
                    if self.fields[field] != other.fields[field]:
                        return False
            return True
        else:
            return False
        
    def __ne__(self, __value: object) -> bool:
        return not self.__eq__(__value)
    
    def __str__(self):
        return self.type