package com.informatik.iamt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.json.JSONTokener;

import netscape.javascript.JSException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

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
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;


import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestBeamPipeline {
  private static final Logger LOG = LogManager.getLogger(TestBeamPipeline.class);
  // private static final Logger LOG = LoggerFactory.getLogger(TestBeamPipeline.class);

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

  public static void main(String[] args) {
    // Create the pipeline options
    IoTStreamingOptions options = PipelineOptionsFactory.fromArgs(args).as(IoTStreamingOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest"); // TODO: make configurable

    // Read from Kafka
    PCollection<KV<String, String>> input1 = pipeline
        .apply("Read from Kafka", KafkaIO.<String, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopicPartitions(
                Collections.singletonList(new TopicPartition("test", 0))
            ) // TODO: support multiple partitions
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            // bound con  
            .withMaxNumRecords(10)
            .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
            .withoutMetadata()) // TODO: support metadata
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(5))));
            
    PCollection<KV<String, String>> input2 = pipeline
        .apply("Read from Kafka", KafkaIO.<String, String>read()
            .withBootstrapServers(options.getKafkaBootstrapServers())
            .withTopicPartitions(
                Collections.singletonList(new TopicPartition("test1", 0))
            ) // TODO: support multiple partitions
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withMaxNumRecords(10)
            .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
            .withoutMetadata()) // TODO: support metadata
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(5))))
        // print out keys and values in system out
        .apply(ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            System.out.println("Processing element: " + c.element());
            c.output(c.element());
          }
        }));

    PCollection<String> merged = PCollectionList.of(input1).and(input2)
        .apply(Flatten.<KV<String, String>>pCollections())
        .apply(Values.create());


    PCollection<Integer> temp = merged.apply("Parse Temperature", 
            ParDo.of(
                new DoFn<String, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    try {
                      LOG.info("Processing element: " + c.element());
                      JSONTokener tokener = new JSONTokener(c.element());
                      JSONObject entry = new JSONObject(tokener);
                      Integer temperature = (Integer) entry.get("temperature");
                      c.output(temperature);
                    } catch (Exception e) {
                      System.out.println("Error parsing JSON: " + e.getMessage()); // TODO: replace with logger
                    }
                  }
                }));
    
    PCollection<Double> avgTemp = temp.apply("Average Temperature", Mean.<Integer>globally().withoutDefaults());

    avgTemp.apply("Format Output", MapElements.into(TypeDescriptors.strings())
      .via(d -> Double.toString(d)))
      .apply("Write to File", TextIO.write().to(options.getOutputFile()).withWindowedWrites());
    
    // Run the pipeline
    pipeline.run();
  }
}