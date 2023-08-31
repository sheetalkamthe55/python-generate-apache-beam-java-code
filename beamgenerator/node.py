import json

class Node:
    def __init__(self, node_id, node: dict, node_info: dict, node_map: dict, source_node_map: dict):
        self.node_id = node_id
        self.node_info = node_info
        self.node_map = node_map
        self.type = node["type"]
        self.name = node_info.get('operation')
        self.parameter_list = node_info.get('parameterList')
        self.node_type = node['type']
        self.next_oiid = node.get('next_oiid') if isinstance(node.get('next_oiid'), list) else [node.get('next_oiid')] if node.get('next_oiid') else []
        self.join_list = node.get('join_list')
        self.input_p_collection_type = []
        self.output_p_collection_type = None

        if self.is_input():
            self.kafka_topic = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
            self.input_json = json.loads(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'inputjson'), None))
            self.window_length_in_sec = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'windowlengthInSec'), None)
            self.sliding_window_step_in_sec = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'slidingWindowStepInSec'), None)
            self.is_sliding_window = True if node['slidingWindowStepInSec'] != None else False
            self.output_p_collection_type = 'TableRow'

    def is_input(self):
        return self.node_type == 'input'
    
    def has_next(self):
        return len(self.next_oiid) > 0
    
    def is_join(self):
        return self.join_list != None

    def is_output(self):
        return self.node_type == 'output'
    
    # define all properties and setters
    @property
    def node_id(self):
        return self.node_id
    
    @property
    def node_info(self):
        return self.node_info
    
    @property
    def node_map(self):
        return self.node_map
    
    @property
    def name(self):
        return self.name
    
    @property
    def parameter_list(self):
        return self.parameter_list
    
    @property
    def node_type(self):
        return self.node_type
    
    @property
    def next_oiid(self):
        return self.next_oiid
    
    @property
    def join_list(self):
        return self.join_list
    
    @property
    def input_p_collection_type(self):
        return self.input_p_collection_type
    
    @property
    def output_p_collection_type(self):
        return self.output_p_collection_type
    
    @node_id.setter
    def node_id(self, node_id):
        self.node_id = node_id
        
    @node_info.setter
    def node_info(self, node_info):
        self.node_info = node_info

    @node_map.setter
    def node_map(self, node_map):
        self.node_map = node_map

    @name.setter
    def name(self, name):
        self.name = name

    @parameter_list.setter
    def parameter_list(self, parameter_list):
        self.parameter_list = parameter_list

    @node_type.setter
    def node_type(self, node_type):
        self.node_type = node_type

    @next_oiid.setter
    def next_oiid(self, next_oiid):
        self.next_oiid = next_oiid

    @join_list.setter
    def join_list(self, join_list):
        self.join_list = join_list

    @input_p_collection_type.setter
    def input_p_collection_type(self, input_p_collection_type):
        self.input_p_collection_type = input_p_collection_type

    @output_p_collection_type.setter
    def output_p_collection_type(self, output_p_collection_type):
        self.output_p_collection_type = output_p_collection_type

    def __str__(self):
        return f'Node: {self.name}'
    
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
