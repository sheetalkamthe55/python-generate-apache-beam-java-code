import argparse
import os
import json
import copy
from typing import cast
from jinja2 import Environment, FileSystemLoader
import beamgenerator
from beamgenerator import Node

# Get the absolute path of the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Create the argument parser
parser = argparse.ArgumentParser(description='Generate an Apache Beam project based on a template')
parser.add_argument('project_name', help='Name of the project')
parser.add_argument('package_name', help='Package name for the project')
parser.add_argument('--target_dir', default='my-project', help='Target directory for the generated project')

# Parse the command-line arguments
args = parser.parse_args()

# Assign the parsed values to variables
project_name = args.project_name
package_name = args.package_name
target_dir = args.target_dir
bootstrap_server_default = "localhost:9092" # TODO: get from model

node_map, source_node_map = beamgenerator.load_nodes_from_model("Flink-Model.json")
 
beam_main = beamgenerator.create_main_header(package_name, source_node_map, bootstrap_server_default)

## parse model tree and create main logic (pipeline code) ##
# 2. loop over all nodes and create transforms
visited_nodes: set[str] = set()
join_nodes: dict[str, Node] = {} # need to be revisted later

# TODO: in case we need original data again, else remove (and remove import of copy)
pending_visit: set[str] = set(source_node_map.keys()) # TODO: maybe don't need this
# start at random source node
current_node = node_map[pending_visit.pop()]

# TODO: make dict access safe -> relevant exceptions
while True:
    # graph traversal is done as follows:
    # 1. repeat until no more source nodes left
    #   a. start at random source node
    #   b. follow path until join node is reached or path ends at output node
    #   c. if join node is reached, add join node to join_nodes and continue with next source node.
    #   d. if path ends at output node, add output node to visited_nodes and continue with next source node.
    # 2. if no more source nodes left, repeat 1. with from join_nodes as start nodes until no more join nodes left

    if current_node.is_join() and not all(node_id in visited_nodes for node_id in current_node.join_list):
        # wait for all parents to be visited and stops current path traversal (later to be continued from join node)
        join_nodes[current_node.node_id] = current_node
        current_node = None 
    
    if current_node:  # i.e. current node is not join node that has unprocessed parents
        print(f"Current processable node: {current_node.node_id}") # TODO: remove
        visited_nodes.add(current_node.node_id)
        if current_node.is_input():
            beam_main += beamgenerator.add_source_java_code(current_node)
        
        elif current_node.is_join():
            join_nodes.pop(current_node.node_id, None)
            beam_main += beamgenerator.add_join_java_code(current_node, node_map)
            
        elif current_node.is_transform(): # not join!
            beam_main += beamgenerator.add_transform_java_code(current_node, node_map)
        
        elif current_node.is_output(): 
            beam_main += beamgenerator.add_sink_java_code(current_node, node_map) 

        if current_node.is_output(): 
            # note that if current node is output it could also be join/transform => need separate case handling          
            current_node = None # terminates current path traversal
        else:
            # continue with child node
            current_node = beamgenerator.get_next_node(current_node, pending_visit, node_map, join_nodes) 
            continue

    # current path is terminated => find new node to continue algorithm
    is_searching_child = True
    while is_searching_child:
        print(f"Pending nodes: {pending_visit}") # TODO: remove
        print(f"Join nodes: {join_nodes}") # TODO: remove
        if len(pending_visit) > 0:
            # continue with next node in pending list
            current_node = node_map[pending_visit.pop()]
            is_searching_child = False
        elif len(join_nodes) > 0:
            # find first join node that can be safely joined
            changed = False
            for join_node_id, join_node in join_nodes.copy().items():
                if all(node_id in visited_nodes for node_id in join_node.join_list):
                    # can be safely joined since all parents have been visited
                    beam_main += beamgenerator.add_join_java_code(join_node, node_map)
                    visited_nodes.add(join_node_id)
                    del join_nodes[join_node_id]
                    changed = True
                    if not join_node.is_output():
                        # continue with child node
                        current_node = beamgenerator.get_next_node(join_node, pending_visit, node_map, join_nodes)
                        is_searching_child = False
                        break
            if not changed:
                raise Exception("No join node can be safely joined and no more nodes to visit left. Graph cannot be transformed into a beam pipeline.")
        else:
            # no more nodes left
            beam_main += f'''
        // Run the pipeline
        pipeline.run();
    }}
}}
'''         
            print(beam_main)
            print("Terminated successfully!")
            exit()



exit()

    # for node in flow['nodes']:
    #     node_id = node['id']
    #     node_type = node['type']
    #     node_name = node['name']
    #     node_inputs = node['inputs']
    #     node_outputs = node['outputs']
    #     node_params = node['parameters']

    #     # create transform for each node type
    #     if node_type == 'filter':
    #         filter_string = f'''
    #     PCollection<InputData{node_inputs[0]}> {node_id} = {node_inputs[0]}
    #             .apply("Filter {node_name}", Filter.by((InputData{node_inputs[0]} input) -> {{
    #                 return input.{node_params['filterField']} {node_params['filterOperator']} {node_params['filterValue']};
    #             }}));
    #         '''
    #         beam_main += filter_string

    #     elif node_type == 'map':
    #         map_string = f'''
    #     PCollection<InputData{node_inputs[0]}> {node_id} = {node_inputs[0]}
    #             .apply("Map {node_name}", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
    #                     .via((InputData{node_inputs[0]} input) -> {{
    #                         return KV.of(input.{node_params['mapField']}, input.{node_params['mapValue']});
    #                     }}));
    #         '''
    #         beam_main += map_string

    #     elif node_type == 'join':
    #         join_string = f'''
    #     PCollection<InputData{node_inputs[0]}> {node_id} = {node_inputs[0]}
    #             .apply("Join {node_name}", Join.innerJoin({node_inputs[1]})
    #                     .on((InputData{node_inputs[0]} input) -> input.{node_params['joinField']}, (InputData{node_inputs[1]} input) -> input.{node_params['joinField']}));
    #         '''
    #         beam_main += join_string

    #     elif node_type == 'union':
    #         union_string = f'''
    #     PCollection<InputData{node_inputs[0]}> {node_id} = PCollectionList.of({node_inputs[0]})
    #             .and({node_inputs[1]})
    #             .apply("Union {node_name}", Flatten.pCollections());
    #         '''
    #         beam_main += union_string

    #     elif node_type == 'groupby':
    #         groupby_string = f'''
    #     PCollection<KV<String, Iterable<InputData{node_inputs[0]}>>> {node_id} = {node_inputs[0]}


# main_class_string = '''
# public class {{PROJECT_NAME}}Pipeline {
#     public static void main(String[] args) {
#         final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
#         run{{PROJECT_NAME}}(options);
#     }
# '''


# code_imple_string = '''
# static void run{{PROJECT_NAME}}(PipelineOptions options) {
#     Pipeline p = Pipeline.create(options);
# '''

kafkastring = '''
    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    p.apply(KafkaIO.<String, String>read()
            .withBootstrapServers("{{BOOTSTRAP_SERVER}}")
            .withTopicPartitions(Collections.singletonList(new TopicPartition("{{TOPIC_NAME}}", 0)))
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withConsumerConfigUpdates(consumerConfig)
            .withMaxNumRecords(10)
            .withoutMetadata())
     .apply(Values.create())
'''

applystring = ''


#since we take input schema from user, we need to convert it and then apply the transform
# using input data is a good idea since we have flexibility to use entire schema or only a few fields
# eg Filter by a field, Group by a field, etc and output the entire schema
applystring = '.apply("Parse JSON to InputData",ParDo.of(new DoFn<String, InputData>() {\n' \
                    '    @ProcessElement\n' \
                    '    public void processElement(ProcessContext c) {\n' \
                    '    String jsonLine = c.element(); \n' \
                    '    InputData inputdata = new Gson().fromJson(jsonLine, InputData.class);\n' \
                    '        c.output(inputdata);\n' \
                    '    }\n' \
                    '}))'


#Element wise Transforms
if transformarg == 'parDo': #Check if can use lambda function
    applystring = applystring + '.apply(ParDo.of(new DoFn<String, String>() {\n' \
                    '    @ProcessElement\n' \
                    '    public void processElement(ProcessContext c) {\n' \
                    '        c.output(c.element());\n' \
                    '    }\n' \
                    '}))'
elif transformarg == 'map': #Check how to select the datatype of lambda function
    applystring = applystring + '.apply(MapElements.into(TypeDescriptors.strings()).via((String line) -> line))'
elif transformarg == 'flatmap': #One to Many mapping eg:splitting a sentence into words, o/p PCollection<String> have to think of other examples
    applystring = applystring + '.apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("\\\\s+"))))'
elif transformarg == 'filter': #o/p of filter is PCollection<InputData>
    applystring = applystring + '.apply("Filter",Filter.by((InputData data) -> data.get{{aggregationField}}() > {{filterarg}}))'

#Other Transforms
elif transformarg == 'flatten':  #o/p PCollection<DataType> by default add flatten if there are multiple PCollections
    applystring = applystring + '.apply(Flatten.iterables())'
if transformarg == 'window': #Take the duration of window as input
    applystring = applystring + '.apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))' 

#Aggregation Transforms
elif transformarg == 'combine':
    applystring = applystring + '.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())'
elif transformarg == 'reshuffle':
    applystring = applystring + '.apply(Reshuffle.viaRandomKey())'
elif transformarg == 'sample':
    applystring = applystring + '.apply(Sample.any({{SAMPLE_SIZE}}))'
elif transformarg == 'distinct':
    applystring = applystring + '.apply(Distinct.create())'
elif transformarg == 'top':
    applystring = applystring + '.apply(Top.of(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
    
#eg: PCollection<KV<String, Datatype>> (need to create a KV)
elif transformarg == 'groupbykey':
    applystring = applystring + '.apply(GroupByKey.create())'
elif transformarg == 'coGroupByKey':
    applystring = applystring + '.apply(CoGroupByKey.create())'
elif transformarg == 'combineperkey':
    applystring = applystring + '.apply(Combine.perKey(Count.<String>combineFn()))'
elif transformarg == 'countperkey': 
    applystring = applystring + '.apply(Count.perKey())'
elif transformarg == 'sumperkey':
    applystring = applystring + '.apply(Sum.perKey())'
elif transformarg == 'minperkey':
    applystring = applystring + '.apply(Min.perKey())'    
elif transformarg == 'maxperkey':
    applystring = applystring + '.apply(Max.perKey())'
elif transformarg == 'meanperkey':
    applystring = applystring + '.apply(Mean.perKey())'
elif transformarg == 'topperkey':   
    applystring = applystring + '.apply(Top.perKey(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
elif transformarg == 'combinegroupedvalues':
    applystring = applystring + '.apply(Combine.groupedValues(Count.<String>combineFn()))'


elif transformarg == 'countglobally': #eg: PCollection<DataType> 
    applystring = applystring + '.apply(Count.globally())'
elif transformarg == 'sumglobally': #eg: PCollection<DataType> Check if to apply with datatype
    applystring = applystring + '.apply(Sum.globally())'
elif transformarg == 'minglobally':   
    applystring = applystring + '.apply(Min.globally())' 
elif transformarg == 'maxglobally':
    applystring = applystring + '.apply(Max.globally())' 
elif transformarg == 'meanglobally':
    applystring = applystring + '.apply(Mean.globally())'
elif transformarg == 'topglobally':
    applystring = applystring + '.apply(Top.globally(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'


 #Final string conversion if writing to the file
convertstring = '.apply("Format InputData to CSV", MapElements\n' \
                ' .into(TypeDescriptors.strings())\n' \
                '.via((InputData inputdata) ->'
for prop in schema['properties'].items():
    convertstring = convertstring + 'inputdata.get' + prop[0].capitalize() + '() + "," +'
convertstring = convertstring[:-7] + '))'
                    

outputstring = '.apply(TextIO.write().to("{{PROJECT_NAME}}"));\n'

runpipeline = 'p.run().waitUntilFinish();}\n'

endBrack = '}'

finalapachecode = f"{import_string}{main_class_string}{code_imple_string}{kafkastring}{applystring}{convertstring}{outputstring}{runpipeline}{endBrack}"

print("finalapachecode: ", finalapachecode)

# Replace PACKAGE_NAME and PROJECT_NAME in finalapachecode
finalapachecode = finalapachecode.replace('{{PACKAGE_NAME}}', package_name)
finalapachecode = finalapachecode.replace('{{PROJECT_NAME}}', project_name)
finalapachecode = finalapachecode.replace('{{BOOTSTRAP_SERVER}}', 'localhost:9092')
finalapachecode = finalapachecode.replace('{{TOPIC_NAME}}', 'echo-input')
finalapachecode = finalapachecode.replace('{{aggregationField}}', aggregationField)
finalapachecode = finalapachecode.replace('{{filterarg}}', filterarg)
finalapachecode = finalapachecode.replace('{{SAMPLE_SIZE}}', '10')


# Load and process the template file
#output = template.render(PACKAGE_NAME=package_name, PROJECT_NAME=project_name)

# Create the target directory
target_path = os.path.join(target_dir, 'src/main/java', package_name.replace('.', '/'))
os.makedirs(target_path, exist_ok=True)


# Write the processed template to the target file
target_file = os.path.join(target_path, f'{project_name}Pipeline.java')

with open(target_file, 'w') as f:
    f.write(finalapachecode)

targetschemafile = os.path.join(target_path, f'InputData.java')

with open(targetschemafile, 'w') as f:
    f.write(class_definition)

# Copy the pom.xml file to the output directory
pom_file_src = os.path.join(current_dir, 'beam-template')
# Create the Jinja2 environment
templateLoader = FileSystemLoader(searchpath=pom_file_src)
templateEnv = Environment(loader=templateLoader)
TEMPLATE_FILE = "pom.xml"
pom_file_srctemplate = templateEnv.get_template(TEMPLATE_FILE)


output2 = pom_file_srctemplate.render(PACKAGE_NAME=package_name, PROJECT_NAME=project_name)
pom_file_dst = os.path.join(target_dir, 'pom.xml')

with open(pom_file_dst, 'w') as f:
    f.write(output2)

