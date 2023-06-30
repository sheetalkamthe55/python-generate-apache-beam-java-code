
# string to create import 
transformarg = 'count'
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
}
'''

codeimplestring = '''
static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);
'''

kafkastring = '''
    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    p.apply(KafkaIO.<Long, String>read()
            .withBootstrapServers({{BOOTSTRAP_SERVER}})
            .withTopicPartitions(Collections.singletonList(new TopicPartition("{{TOPIC_NAME}}", 0)))
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withConsumerConfigUpdates(consumerConfig)
            .withMaxNumRecords(5)
            .withoutMetadata())
     .apply(Values.create())
'''

applystring = ''
if transformarg == 'count':
    applystring = '.apply(Count.perElement())'

outputstring = '.apply(TextIO.write().to("{{PROJECT_NAME}}"));\n'

runpipeline = 'p.run().waitUntilFinish();\n'

endBrack = '}'

finalapachecode = f"{importstring}{mainclassstring}{codeimplestring}{kafkastring}{applystring}{outputstring}{runpipeline}{endBrack}"

print("finalapachecode: ", finalapachecode)
