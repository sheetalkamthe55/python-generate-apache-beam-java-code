import argparse
import os
import json
import copy
from jinja2 import Environment, FileSystemLoader

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

# Load the JSON Model
with open(("ME-Model.json"), 'r') as f:
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
            "is_join": True if node.get('join_list') else False
        }
        if node_map[node_id]['type'] == 'input':
            node_map[node_id]['kafkaTopic'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'kafkaTopic'), None)
            node_map[node_id]['inputjson'] = json.loads(next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'inputjson'), None))
            node_map[node_id]['windowlengthInSec'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'windowlengthInSec'), None)
            node_map[node_id]['slidingWindowStepInSec'] = next((paramter['value'] for paramter in node_info['parameter_list'] if paramter['name'] == 'slidingWindowStepInSec'), None)
            node_map[node_id]['slidingWindow'] = True if node_map[node_id]['slidingWindowStepInSec'] != None else False
            source_node_map[node_id] = node_map[node_id]  


# Create input schema classes # TODO: move to separate file # TODO: accept different types (array, boolean, integer, null)
input_schema_classes = ''
for source_node_id, source_node in source_node_map.items():
    schema = source_node['inputjson']
    # Loop through the properties and generate the class fields and annotations
    fields = []
    methods = []
    for prop, prop_schema in schema['properties'].items():
        field_name = prop
        field_type = 'String' if prop_schema['type'] == 'string' else 'Double'
        annotation = f'@SerializedName("{prop}")'
        field = f'\n        {annotation}\n        private {field_type} {field_name};\n'
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
    public class InputData{source_node_id} implements Serializable {{
    
        public InputData{source_node_id}() {{}};
        {class_fields}
        {class_methods}
    }}

'''


## Imports and utility for pipeline ## # TODO: maybe move to separate file (also create schema classes in separate files)
# imports 
beam_main = ''

import_string = '''
package {{PACKAGE_NAME}};

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.joda.time.Duration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.delta.standalone.DeltaLog;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;

import {{PACKAGE_NAME}}.InputData;
import com.google.gson.Gson;
import org.apache.beam.sdk.values.TypeDescriptors;

'''

# main class
beam_main += import_string
beam_main += '''
public class TestBeamPipeline {

'''

# input schema subclasses
beam_main += input_schema_classes

# streaming options interface, kafka read transform, window transform and main method header
beam_main += '''
    private static final Logger LOG = LogManager.getLogger(TestBeamPipeline.class);

    public interface IoTStreamingOptions extends PipelineOptions {

        @Description("Kafka bootstrap servers, comma separated list")
        @Default.String("localhost:9092")
        String getKafkaBootstrapServers();

        void setKafkaBootstrapServers(String kafkaBootstrapServers);

        @Description("Kafka topic to read from")
        @Required
        String getKafkaTopic();

        void setKafkaTopic(String kafkaTopic);

        @Description("Path of the file to write to")
        @Default.String("output.txt")
        String getOutputFile();

        void setOutputFile(String outputFile);
    }

    private static PTransform<@UnknownKeyFor @NonNull @Initialized PBegin, @UnknownKeyFor @NonNull @Initialized PCollection<@UnknownKeyFor @NonNull @Initialized KV<String, String>>> kafkaRead(IoTStreamingOptions options, String test, Map<String, Object> consumerConfig) {
        return KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopicPartitions(
                        Collections.singletonList(new TopicPartition(test, 0))
                )
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
                .withoutMetadata();
    }

    private static <T> Window<T> window(long duration, boolean sliding, long period) {
        Window<T> window;
        if (sliding) {
            window = Window.<T>into(SlidingWindows.of(Duration.standardSeconds(duration)).every(Duration.standardSeconds(period)));
        } else {
            window = Window.<T>into(FixedWindows.of(Duration.standardSeconds(duration)));
        }
        return window.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1))))
            .withAllowedLateness(Duration.standardSeconds(5))
            .discardingFiredPanes();
    }

    public static void main(String[] args) {
        // Create the pipeline options
        IoTStreamingOptions options = PipelineOptionsFactory.fromArgs(args).as(IoTStreamingOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

'''

## parse model tree and create pipeline code ##

# 1. create source node code
def add_source_node(source_node_id, source_node): 
    return f'''
        PCollection<InputData> {source_node_id} = pipeline
            .apply("Read from Kafka", kafkaRead(options, "{source_node['kafkaTopic']}", consumerConfig))
            .apply(Values.create())
            .apply(window({source_node['windowlengthInSec']}, {source_node['slidingWindow']}, {source_node['slidingWindowStepInSec']}))
            .apply("Parse JSON to InputData{source_node_id}", ParDo.of(new DoFn<String, InputData{source_node_id}>() {{
                @ProcessElement
                public void processElement(ProcessContext c) {{
                    String jsonLine = c.element(); 
                    InputData{source_node_id} inputdata = new Gson().fromJson(jsonLine, InputData{source_node_id}.class);
                    c.output(inputdata);
                }}
            }}));

    '''

def add_join_node(join_node_id, join_node):
    # TODO: implement join logic
    return f'''
        PCollection<InputData> {join_node_id};
    '''

def add_transform_node(transform_node_id, transform_node):
    # TODO: implement transform logic
    return f'''
        PCollection<InputData> {transform_node_id};
    '''

def add_sink_node(sink_node_id, sink_node):
    # TODO. implement sink node logic
    return f'''
        PCollection<InputData> {sink_node_id};
    '''


def get_next_node(node, pending_visit):
    if len(node["next_oiid"]) > 1:
        pending_visit.add(node["next_oiid"][1:])
    elif len(node["next_oiid"]) < 1:
        raise Exception("Node has no next node")
    else:
        return node["next_oiid"][0]

    
# 2. loop over all nodes and create transforms
# for flow in model['flows']: # TODO: do we need this?
visited_nodes = {}
join_nodes = {} # need to be revisted later

# TODO: in case we need original data again, else remove (and remove import of copy)
pending_visit_map = source_node_map.copy()

# start at random source node
current_node_id, current_node = source_node_map.popitem()

print(beam_main)

# TODO: make dict access safe -> relevant exceptions
while len(pending_visit_map) > 0 or len(join_nodes) > 0 or len(node_map) > 0:
    # graph traversal is done as follows:
    # 1. repeat until no more source nodes left
    #   a. start at random source node
    #   b. follow path until join node is reached or path ends at output node
    #   c. if join node is reached, add join node to join_nodes and continue with next source node.
    #   d. if path ends at output node, add output node to visited_nodes and continue with next source node.
    # 2. if no more source nodes left, repeat 1. with from join_nodes as start nodes until no more join nodes left


    if current_node["type"] == "source":
        beam_main += add_source_node(current_node_id, current_node)
        visited_nodes[current_node_id] = current_node
        current_node_id, current_node = get_next_node(current_node, pending_visit_map) # child of current
    elif current_node["is_join"]:
        if all(node in visited_nodes for node in current_node["join_list"]):
            # can be safely joined since all parents have been visited
            beam_main += add_join_node(current_node_id, current_node)
            visited_nodes[current_node_id] = current_node
            current_node_id, current_node = get_next_node(current_node, pending_visit_map) # child of current
        else:
            # wait for all parents to be visited
            join_nodes[current_node_id] = current_node
            if len(pending_visit_map) > 0:
                current_node_id, current_node = pending_visit_map.popitem()
            else:
                # find first join node that can be safely joined
                changed = False
                for join_node_id, join_node in join_nodes.items():
                    if all(node in visited_nodes for node in join_node["join_list"]):
                        # can be safely joined since all parents have been visited
                        beam_main += add_join_node(join_node_id, join_node)
                        visited_nodes[join_node_id] = join_node
                        current_node_id, current_node = get_next_node(join_node, pending_visit_map)
                        changed = True
                        break
                if not changed:
                    raise Exception("No join node can be safely joined and no more nodes to visit left. Graph cannot be transformed into a beam pipeline.")
    
            


    
    if len(pending_visit_map) > 0:
        current_node_id, current_node = pending_visit_map.popitem()
    elif len(node_map) > 0:
        # TODO: 
        pass





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

