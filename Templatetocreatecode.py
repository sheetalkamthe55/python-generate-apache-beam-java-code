import argparse
import os
from jinja2 import Environment, FileSystemLoader

# Get the absolute path of the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Create the argument parser
parser = argparse.ArgumentParser(description='Generate an Apache Beam project based on a template')
parser.add_argument('project_name', help='Name of the project')
parser.add_argument('package_name', help='Package name for the project')
parser.add_argument('--target_dir', default='my-project', help='Target directory for the generated project')
parser.add_argument('transformarg', help='Get the name of transformations to be applied')

# Parse the command-line arguments
args = parser.parse_args()

# Assign the parsed values to variables
project_name = args.project_name
package_name = args.package_name
target_dir = args.target_dir
transformarg = args.transformarg
importstring = '''
package {{PACKAGE_NAME}};

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import {{PACKAGE_NAME}}.InputData;
'''

mainclassstring = '''
public class {{PROJECT_NAME}}Pipeline {
    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        run{{PROJECT_NAME}}(options);
    }
'''

codeimplestring = '''
static void run{{PROJECT_NAME}}(PipelineOptions options) {
    Pipeline p = Pipeline.create(options);
'''

kafkastring = '''
    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    p.apply(KafkaIO.<Long, String>read()
            .withBootstrapServers("{{BOOTSTRAP_SERVER}}")
            .withTopicPartitions(Collections.singletonList(new TopicPartition("{{TOPIC_NAME}}", 0)))
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            //.withConsumerConfigUpdates(consumerConfig)
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
                    '        c.output(c.element());\n' \
                    '    }\n' \
                    '}))'


#Element wise Transforms
if transformarg == 'parDo': #Check if can use lambda function
    applystring = '.apply(ParDo.of(new DoFn<String, String>() {\n' \
                    '    @ProcessElement\n' \
                    '    public void processElement(ProcessContext c) {\n' \
                    '        c.output(c.element());\n' \
                    '    }\n' \
                    '}))'
elif transformarg == 'map': #Check how to select the datatype of lambda function
    applystring = '.apply(MapElements.into(TypeDescriptors.strings()).via((String line) -> line))'
elif transformarg == 'flatmap': #One to Many mapping eg:splitting a sentence into words, o/p PCollection<String> have to think of other examples
    applystring = '.apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("\\\\s+"))))'
elif transformarg == 'filter': #o/p of filter is PCollection<InputData>
    applystring = '.apply(Filter.by((InputData data) -> data.{{GETFILTER_FIELD}} > {{FILTER_VALUE}}))'

#Other Transforms
elif transformarg == 'flatten':  #o/p PCollection<DataType>
    applystring = '.apply(Flatten.iterables())'
if transformarg == 'window': #Take the duration of window as input
    applystring = '.apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))' 

#Aggregation Transforms
elif transformarg == 'combine':
    applystring = '.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())'
elif transformarg == 'reshuffle':
    applystring = '.apply(Reshuffle.viaRandomKey())'
elif transformarg == 'sample':
    applystring = '.apply(Sample.any({{SAMPLE_SIZE}}))'
elif transformarg == 'distinct':
    applystring = '.apply(Distinct.create())'
elif transformarg == 'top':
    applystring = '.apply(Top.of(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
    
#eg: PCollection<KV<String, Datatype>> (need to create a KV)
elif transformarg == 'groupbykey':
    applystring = '.apply(GroupByKey.create())'
elif transformarg == 'coGroupByKey':
    applystring = '.apply(CoGroupByKey.create())'
elif transformarg == 'combineperkey':
    applystring = '.apply(Combine.perKey(Count.<String>combineFn()))'
elif transformarg == 'countperkey': 
    applystring = '.apply(Count.perKey())'
elif transformarg == 'sumperkey':
    applystring = '.apply(Sum.perKey())'
elif transformarg == 'minperkey':
    applystring = '.apply(Min.perKey())'    
elif transformarg == 'maxperkey':
    applystring = '.apply(Max.perKey())'
elif transformarg == 'meanperkey':
    applystring = '.apply(Mean.perKey())'
elif transformarg == 'topperkey':   
    applystring = '.apply(Top.perKey(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
elif transformarg == 'combinegroupedvalues':
    applystring = '.apply(Combine.groupedValues(Count.<String>combineFn()))'


elif transformarg == 'countglobally': #eg: PCollection<DataType> 
    applystring = '.apply(Count.globally())'
elif transformarg == 'sumglobally': #eg: PCollection<DataType> Check if to apply with datatype
    applystring = '.apply(Sum.globally())'
elif transformarg == 'minglobally':   
    applystring = '.apply(Min.globally())' 
elif transformarg == 'maxglobally':
    applystring = '.apply(Max.globally())' 
elif transformarg == 'meanglobally':
    applystring = '.apply(Mean.globally())'
elif transformarg == 'topglobally':
    applystring = '.apply(Top.globally(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
    
                    

outputstring = '.apply(TextIO.write().to("{{PROJECT_NAME}}"));\n'

runpipeline = 'p.run().waitUntilFinish();}\n'

endBrack = '}'

finalapachecode = f"{importstring}{mainclassstring}{codeimplestring}{kafkastring}{applystring}{outputstring}{runpipeline}{endBrack}"

print("finalapachecode: ", finalapachecode)

# Replace PACKAGE_NAME and PROJECT_NAME in finalapachecode
finalapachecode = finalapachecode.replace('{{PACKAGE_NAME}}', package_name)
finalapachecode = finalapachecode.replace('{{PROJECT_NAME}}', project_name)
finalapachecode = finalapachecode.replace('{{BOOTSTRAP_SERVER}}', 'localhost:9092')
finalapachecode = finalapachecode.replace('{{TOPIC_NAME}}', 'echo-input')


# Load and process the template file
#output = template.render(PACKAGE_NAME=package_name, PROJECT_NAME=project_name)

# Create the InputSchemaClass.java file
import json
# Load the JSON schema
with open(("schema.json"), 'r') as f:
    schema = json.load(f)

# Loop through the properties and generate the class fields and annotations
fields = []
methods = []
for prop, prop_schema in schema['properties'].items():
    field_name = prop
    field_type = 'String' if prop_schema['type'] == 'string' else 'Double'
    annotation = f'@SerializedName("{prop}")'
    field = f'{annotation}\nprivate {field_type} {field_name};\n'
    fields.append(field)

    method_name = prop
    method_type = 'String' if prop_schema['type'] == 'string' else 'Double'
    method = f'''public {method_type} get{method_name.capitalize()}() {{
        return {prop};
    }}

    public void set{method_name.capitalize()}({method_type} {prop}) {{
        this.{prop} = {prop};
    }}
    '''
    methods.append(method)

# Combine the fields into a single string
class_fields = '\n'.join(fields)

# Combine the methods into a single string
class_methods = '\n'.join(methods)


# Generate the class definition with fields and getter/setter methods
class_definition = f'''package {{PACKAGE_NAME}};

import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
     
    public class InputData implements Serializable{{
    
    public InputData() {

    ''}

    {class_fields}

    {class_methods}
}}
'''

# Print the class fields
print(class_definition)



# Create the target directory
target_path = os.path.join(target_dir, 'src/main/java', package_name.replace('.', '/'))
os.makedirs(target_path, exist_ok=True)


# Write the processed template to the target file
target_file = os.path.join(target_path, f'{project_name}Pipeline.java')

with open(target_file, 'w') as f:
    f.write(finalapachecode)

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


#shutil.copy2(pom_file_src, pom_file_dst)
