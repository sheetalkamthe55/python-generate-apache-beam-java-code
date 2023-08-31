import json

def load_nodes_from_model(model_path):
    with open((model_path), 'r') as f:
        model = json.load(f)

        node_map = {}
        source_node_map = {}
        for node_id, node in model['flows'][0]['flow'].items():
            node_info = next((item for item in model['streaming'] if item['flowElementId'] == node_id), None)
            node_map[node_id] = {
                "name": node.get('operation'), # Note that this is the transform operation for transforms and source name for sources
                "parameter_list": node_info.get('parameterList'),
                "type": node['type'],
                "next_oiid": node.get('next_oiid') if isinstance(node.get('next_oiid'), list) else [node.get('next_oiid')],
                "join_list": node.get('join_list'),
                "input_p_collection_type": [], 
                "output_p_collection_type": None
            }
            if node_map[node_id]['type'] == 'input':
                node_map[node_id]['kafkaTopic'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
                node_map[node_id]['inputjson'] = json.loads(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'inputjson'), None))
                node_map[node_id]['windowlengthInSec'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'windowlengthInSec'), None)
                node_map[node_id]['slidingWindowStepInSec'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'slidingWindowStepInSec'), None)
                node_map[node_id]['slidingWindow'] = True if node_map[node_id]['slidingWindowStepInSec'] != None else False
                source_node_map[node_id] = node_map[node_id]  
        return node_map, source_node_map
    
def create_input_schema_classes(source_node_map):
    # TODO: accept different types (array, boolean, integer, null)
    input_schema_classes = ''
    for source_node_id, source_node in source_node_map.items():
        source_node["output_p_collection_type"] = f'InputData{source_node_id.capitalize()}'
        source_node["fields_list"] = []
        schema = source_node['inputjson']
        # Loop through the properties and generate the class fields and annotations
        fields = []
        methods = []
        for prop, prop_schema in schema['properties'].items():
            source_node["fields_list"].append(prop)
            field_type = 'String' if prop_schema['type'] == 'string' else 'Double'
            annotation = f'@SerializedName("{prop}")'
            field = f'\n        {annotation}\n        private {field_type} {prop};\n'
            fields.append(field)

            method_name = prop
            method_type = 'String' if prop_schema['type'] == 'string' else 'Double'
            method = f'''\n        public {method_type} get{method_name.capitalize()}() {{
            return {prop};
        }}

        public void set{method_name.capitalize()}({method_type} {prop}) {{
            this.{prop} = {prop};
        }}
            '''
            methods.append(method)

        # Combine the fields into a single string
        class_fields = ''.join(fields)

        # Combine the methods into a single string
        class_methods = ''.join(methods)

        input_schema_classes += f'''
    @DefaultCoder(AvroCoder.class) 
    public class {source_node["output_p_collection_type"]} implements Serializable {{
    
        public {source_node["output_p_collection_type"]}() {{}};
        {class_fields}
        {class_methods}
    }}

        '''
    return input_schema_classes