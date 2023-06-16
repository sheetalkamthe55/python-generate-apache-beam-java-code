package {{PACKAGE_NAME}};

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;

public class {{PROJECT_NAME}}Pipeline {

  public static void main(String[] args) {
    // Create the pipeline options
    MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Read from Kafka
    pipeline
        .apply("Read from Kafka", KafkaIO.<String, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopic(options.getKafkaTopic())
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class))
        .apply("Extract Words", ParDo.of(new ExtractWordsFn()))
        .apply(Count.perElement())
        .apply("Format Output", MapElements.into(TypeDescriptors.strings())
            .via(kv -> kv.getKey() + ": " + kv.getValue()))
        .apply("Write to File", TextIO.write().to(options.getOutputFile()));

    // Run the pipeline
    pipeline.run();
  }
}
