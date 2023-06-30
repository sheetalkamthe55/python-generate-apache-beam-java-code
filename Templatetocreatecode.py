import argparse
import os
import shutil
from jinja2 import Environment, FileSystemLoader
import CreateClassApache

# Get the absolute path of the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the template file

path=os.path.join(current_dir, 'beam-template', 'src', 'main', 'java', '{{PACKAGE_NAME}}')

# Create the Jinja2 environment
templateLoader = FileSystemLoader(searchpath=path)
templateEnv = Environment(loader=templateLoader)
TEMPLATE_FILE = "{{PROJECT_NAME}}Pipeline.java"

# Load the template file
template = templateEnv.get_template(TEMPLATE_FILE)

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
elif transformarg == 'flatmap': #Check how to select the datatype of lambda function, Typedescriptors 
    applystring = '.apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("\\\\s+"))))'
elif transformarg == 'filter': #o/p of filter is PCollection<DataTypes>
    applystring = '.apply(Filter.by((String line) -> line.length() > 0))'

#Other Transforms
elif transformarg == 'flatten':  #o/p PCollection<DataType>
    applystring = '.apply(Flatten.iterables())'
if transformarg == 'window': #Take the duration of window as input
    applystring = '.apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))))' 

#Aggregation Transforms
elif transformarg == 'combine':
    applystring = '.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults())'
elif transformarg == 'groupbykey':
    applystring = '.apply(GroupByKey.create())'
elif transformarg == 'coGroupByKey':
    applystring = '.apply(CoGroupByKey.create())'
elif transformarg == 'join':
    applystring = '.apply(Join.innerJoin(PCollectionList.of(p.apply(TextIO.read().from("{{PROJECT_NAME}}1.txt"))).and(p.apply(TextIO.read().from("{{PROJECT_NAME}}2.txt")))).by(KV::getKey))'
elif transformarg == 'union':
    applystring = '.apply(Union.<String>of(p.apply(TextIO.read().from("{{PROJECT_NAME}}1.txt")), p.apply(TextIO.read().from("{{PROJECT_NAME}}2.txt"))))'
elif transformarg == 'reshuffle':
    applystring = '.apply(Reshuffle.viaRandomKey())'
elif transformarg == 'sample':
    applystring = '.apply(Sample.any(1))'
elif transformarg == 'distinct':
    applystring = '.apply(Distinct.create())'
elif transformarg == 'top':
    applystring = '.apply(Top.of(1, new Comparator<String>() {\n' \
                    '    @Override\n' \
                    '    public int compare(String o1, String o2) {\n' \
                    '        return o1.compareTo(o2);\n' \
                    '    }\n' \
                    '}))'
elif transformarg == 'combineperkey':
    applystring = '.apply(Combine.perKey(Count.<String>combineFn()))'
elif transformarg == 'countperkey': #eg: PCollection<KV<String, Long>>
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
