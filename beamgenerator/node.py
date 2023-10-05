from copy import deepcopy
import json

from .types import JAVA_NUMERIC_TYPE, AbstractPCollectionType, InputPCollectionType

class Node:

    def __init__(self, node_id, node: dict, node_info: dict):
        self.input_p_collection_types: list[AbstractPCollectionType] = []
        self.output_p_collection_type: AbstractPCollectionType = None
        self.node_id: str = node_id
        self.type: str = node_info["type"]
        self.name: str = node_info["name"] # Note this is the transform operation for transforms and source name for sources
        # for transform this is one in ["Mean", "Flatmap", "Map", "Filter", "Window"]
        # for streamoutput this is one in ["KafkaOutput", "DeltaOutput"]
        self.next_oiid: list[str] = [] if (node.get('next_oiid') == "none") else node.get('next_oiid') if isinstance(node.get('next_oiid'), list) else [node.get('next_oiid')] if node.get('next_oiid') else []
        self.join_list: list[str] = node.get('join_list') 
        self.parameter_list: dict[str, str] = node_info['parameter_list']
        
        self.kafka_topic: str = None
        self.input_json: str = None
        self.window_length_in_sec: str = None
        self.sliding_window_step_in_sec: str = None
        if self.is_input():
            self.kafka_topic: str = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
            self.input_json: str = json.loads(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'inputjson'), None)) # TODO: remove? redundant information
            self.window_length_in_sec: str = str(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'windowlengthInSec'), "null")) # TODO: create own window class?
            self.sliding_window_step_in_sec: str = str(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'slidingWindowStepInSec'), "null"))
            self.output_p_collection_type = InputPCollectionType(f'InputData{self.node_id.capitalize()}', self.node_id)
            for prop, prop_schema in self.input_json['properties'].items():
                property_type = prop_schema['type'].capitalize()
                property_type = 'String' if property_type == 'Str' else property_type
                if property_type not in JAVA_NUMERIC_TYPE + ["String"]:
                    raise Exception(f'Property type {property_type} for {prop} in inputjson in node {self.name} not supported!')
                self.output_p_collection_type.add_field((prop, property_type))
        
        elif self.is_output():
            if self.name == 'KafkaOutput':
                self.kafka_topic: str = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
            elif self.name == 'DeltaOutput':
                # TODO
                pass
            elif self.name == 'FileOutput':
                self.output_file: str = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'outputFile'), None)

    def is_input(self):
        return self.type == 'streaminput'
    
    def is_join(self):
        return self.join_list != None and len(self.join_list) > 1
    
    def is_transform(self):
        return self.type == 'transform'

    def is_output(self):
        return self.type == 'streamoutput' or self.next_oiid == None or len(self.next_oiid) == 0 or self.next_oiid == "none"

    def add_input_p_collection_type(self, input_p_collection_type: AbstractPCollectionType, name: str = None):
        if name:
            print(f'Adding input_p_collection_type {name} {input_p_collection_type} to node {self.node_id}')
            input_p_collection_type = deepcopy(input_p_collection_type)
            input_p_collection_type.name = name
        self.input_p_collection_types.append(input_p_collection_type)
        print(f'Input types of {self.node_id}: {self.input_p_collection_types}')
        # we don't care about duplicates
    
    def is_sliding_window(self):
        """Returns Java boolean string representation of whether the node is a sliding window node or not"""
        return "true" if self.window_length_in_sec != "null" and self.sliding_window_step_in_sec != "null" else "false"

    def get_sliding_window_value(self):
        if self.sliding_window_step_in_sec == "null" or not self.sliding_window_step_in_sec:
            return 0
        else:
            return self.sliding_window_step_in_sec
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
