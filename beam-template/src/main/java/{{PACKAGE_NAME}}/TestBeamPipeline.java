package com.informatik.iamt;

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
import org.joda.time.Duration;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.delta.standalone.DeltaLog;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;


public class TestBeamPipeline {

    @DefaultCoder(AvroCoder.class)
    public class InputData implements Serializable {

        public InputData() {};

        @SerializedName("component")
        private String component;

        @SerializedName("id")
        private String id;

        @SerializedName("temperature")
        private Double temperature;


        public String getComponent() {
            return component;
        }

        public void setComponent(String component) {
            this.component = component;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Double getTemperature() {
            return temperature;
        }

        public void setTemperature(Double temperature) {
            this.temperature = temperature;
        }

    }
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

    private static PTransform<@UnknownKeyFor @NonNull @Initialized PBegin, @UnknownKeyFor @NonNull @Initialized PCollection<@UnknownKeyFor @NonNull @Initialized KV<String, String>>> kafkaRead(IoTStreamingOptions options, String test, Map<String, Object> consumerConfig) {
        return KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopicPartitions(
                        Collections.singletonList(new TopicPartition(test, 0))
                ) // TODO: support multiple partitions
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
                .withoutMetadata();
    }

    private static <T> Window<T> window() {
        return Window.<T>into(FixedWindows.of(Duration.standardSeconds(10)))
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10))))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes();
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
                .apply("Read from Kafka", kafkaRead(options, "test1", consumerConfig)); // TODO: support metadata
        PCollection<KV<String, String>> input2 = pipeline
                .apply("Read from Kafka", kafkaRead(options, "test2", consumerConfig)); // TODO: support metadata

        PCollection<String> mergedColl = PCollectionList.of(input1).and(input2)
                .apply(Flatten.<KV<String, String>>pCollections())
                .apply(window())
                .apply(Values.create());

        PCollection<InputData> inputDataColl = mergedColl.apply("Parse JSON to InputData", ParDo.of(new DoFn<String, InputData>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String jsonLine = c.element();
                InputData inputdata = new Gson().fromJson(jsonLine, InputData.class);
                c.output(inputdata);
            }
        }));

        // group by key
        // PCollection<KV<String, Iterable<String>>> grouped = merged.apply(GroupByKey.create());


        PCollection<Double> tempColl = inputDataColl.apply("Parse Temperature", MapElements.into(TypeDescriptors.doubles())
                        .via((InputData inputData) -> inputData.getTemperature().doubleValue()));

        PCollection<Double> avgColl = tempColl.apply("Average Temperature in Window", Mean.<Double>globally().withoutDefaults());

        // TODO: write output to delta lake
        avgColl.apply("Format Output", MapElements.into(TypeDescriptors.strings())
            .via(d -> Double.toString(d)))
            .apply("Write to File", TextIO.write().to(options.getOutputFile()).withWindowedWrites());
        
            // use delta lake standalone https://docs.delta.io/latest/delta-standalone.html


        // Run the pipeline
        pipeline.run();
    }
}