import json
from typing import ClassVar

from .types import AbstractPCollectionType, InputPCollectionType

class Node:
    node_id: ClassVar[str] = ""
    type: ClassVar[str] = ""
    name: ClassVar[str] = ""
    next_oiid: ClassVar[list[str]] = []
    join_list: ClassVar[list[str]] = []
    input_p_collection_types: ClassVar[list[AbstractPCollectionType]] = []
    output_p_collection_type: ClassVar[AbstractPCollectionType] = None
    kafka_topic: ClassVar[str] = None
    input_json: ClassVar[str] = None
    window_length_in_sec: ClassVar[str] = None
    sliding_window_step_in_sec: ClassVar[str] = None

    def __init__(self, node_id, node: dict, node_info: dict):
        self.node_id: str = node_id
        self.type: str = node_info["type"]
        self.name: str = node_info["name"] # Note this is the transform operation for transforms and source name for sources
        # for transform this is one in ["Mean", "Flatmap", "Map", "Filter", "Window"]
        # for streamoutput this is one in ["KafkaOutput", "DeltaOutput"]
        self.next_oiid: list[str] = [] if (node.get('next_oiid') == "none") else node.get('next_oiid') if isinstance(node.get('next_oiid'), list) else [node.get('next_oiid')] if node.get('next_oiid') else []
        self.join_list: list[str] = node.get('join_list') 

        if self.is_input():
            self.kafka_topic: str = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
            self.input_json: str = json.loads(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'inputjson'), None)) # TODO: remove? redundant information
            self.window_length_in_sec: str = str(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'windowlengthInSec'), "null")) # TODO: create own window class?
            self.sliding_window_step_in_sec: str = str(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'slidingWindowStepInSec'), "null"))
            self.output_p_collection_type = InputPCollectionType(f'InputData{self.node_id.capitalize()}')
            for prop, prop_schema in self.input_json['properties'].items():
                property_type = prop_schema['type']
                property_type = 'String' if property_type == 'string' else 'Double' # TODO: accept different types (array, boolean, integer, null)?
                self.output_p_collection_type.add_field((prop, prop_schema['type']))

    def is_input(self):
        return self.type == 'streaminput'
    
    def is_join(self):
        return self.join_list != None and len(self.join_list) > 1
    
    def is_transform(self):
        return self.type == 'transform'

    def is_output(self):
        return self.type == 'streamoutput' or self.next_oiid == None or len(self.next_oiid) == 0 or self.next_oiid == "none"

    def add_input_p_collection_type(self, input_p_collection_type: AbstractPCollectionType):
        self.input_p_collection_types.append(input_p_collection_type)
    
    def is_sliding_window(self):
        """Returns Java boolean string representation of whether the node is a sliding window node or not"""
        return "true" if self.window_length_in_sec != "null" and self.sliding_window_step_in_sec != "null" else "false"

    def __str__(self):
        return f'Node: {self.node_id} {self.name} ({self.type}) with next_oiid: {self.next_oiid} and join_list: {self.join_list}'
    
    # TODO: maybe remove all the following methods?
    def __repr__(self):
        return f'Node: {self.name}'
    
    def __eq__(self, other):
        return self.node_id == other.node_id
    
    def __hash__(self):
        return hash(self.node_id)
    
    def __lt__(self, other):
        return self.node_id < other.node_id
    
    def __gt__(self, other):
        return self.node_id > other.node_id
    
    def __le__(self, other):
        return self.node_id <= other.node_id
    
    def __ge__(self, other):
        return self.node_id >= other.node_id
    
    def __ne__(self, other):
        return self.node_id != other.node_id
    
    def __cmp__(self, other):
        return self.node_id.__cmp__(other.node_id)
