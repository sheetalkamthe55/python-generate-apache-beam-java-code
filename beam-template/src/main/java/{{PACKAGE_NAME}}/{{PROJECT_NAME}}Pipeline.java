package {{PACKAGE_NAME}};

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class {{PROJECT_NAME}}Pipeline {

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

  void setOutputFile(String outputFile);}

  public static void main(String[] args) {
    // Create the pipeline options
    IoTStreamingOptions options = PipelineOptionsFactory.fromArgs(args).as(IoTStreamingOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest"); // TODO: make configurable

    // Read from Kafka
    pipeline
        .apply("Read from Kafka", KafkaIO.<String, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopicPartitions(
                Collections.singletonList(new TopicPartition(options.getKafkaTopic(), 0))
            ) // TODO: support multiple partitions
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
            .withMaxNumRecords(5) // TODO: remove this?
            .withoutMetadata()) // TODO: support metadata
        .apply(Values.create())
        .apply("Extract Words", 
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    for (String word : c.element().split("[^\\p{L}]+", 0)) {
                      if (!word.isEmpty()) {
                        c.output(word);
                      }
                    }
                  }
                }))
        // .apply(Count.perElement())
        // .apply("Format Output", MapElements.into(TypeDescriptors.strings())
        //     .via(kv -> kv.getKey() + ": " + kv.getValue()))
        .apply("Write to File", TextIO.write().to(options.getOutputFile()));

    // Run the pipeline
    pipeline.run();
  }
}
