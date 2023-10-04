import json
from typing import cast

from beamgenerator.types import InputPCollectionType

from .node import Node

def load_nodes_from_model(model_path) -> tuple[dict[str, Node], dict[str, Node]]:
    with open((model_path), 'r') as f:
        model = json.load(f)

        node_map = {}
        source_node_map = {}
        for node_id, node in model['flows'][0]['flow'].items(): # we assume that flows only contains one flow
            node_info = next((item for item in model['streaming'] if item['id'] == node_id), None)
            node_map[node_id] = Node(node_id, node, node_info)
            if node_map[node_id].is_input():
                source_node_map[node_id] = node_map[node_id]
        return node_map, source_node_map
    
def create_input_schema_classes(source_node_map: dict[str, Node]):
    input_schema_classes = ''
    for source_node in source_node_map.values():
        # Loop through the properties and generate the class fields and annotations
        fields = []
        methods = []
        for property_name, property_type in cast(InputPCollectionType, source_node.output_p_collection_type).fields:
            annotation = f'@SerializedName("{property_name}")'
            field = f'\n        {annotation}\n        private {property_type} {property_name};\n'
            fields.append(field)
            method = f'''\n        public {property_type} get{property_name.capitalize()}() {{
            return {property_name};
        }}

        public void set{property_name.capitalize()}({property_type} {property_name}) {{
            this.{property_name} = {property_name};
        }}
            '''
            methods.append(method)

        # Combine the fields into a single string
        class_fields = ''.join(fields)

        # Combine the methods into a single string
        class_methods = ''.join(methods)

        input_schema_classes += f'''
    @DefaultCoder(AvroCoder.class) 
    public class {source_node.output_p_collection_type.name} implements Serializable {{
    
        public {source_node.output_p_collection_type.name}() {{}};
        {class_fields}
        {class_methods}
    }}

        '''
    return input_schema_classes