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
import org.apache.kafka.common.serialization.StringSerializer;

import io.delta.standalone.DeltaLog;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.google.gson.Gson;
import org.apache.beam.sdk.values.TypeDescriptors;


public class TestBeamPipeline1 {


    @DefaultCoder(AvroCoder.class)
    public class InputDataNode0 implements Serializable {

        public InputDataNode0() {};

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


    @DefaultCoder(AvroCoder.class)
    public class InputDataNode1 implements Serializable {

        public InputDataNode1() {};

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

    private static PTransform<@UnknownKeyFor @NonNull @Initialized PBegin, @UnknownKeyFor @NonNull @Initialized PCollection<@UnknownKeyFor @NonNull @Initialized KV<String, String>>> kafkaRead(IoTStreamingOptions options, String topic, Map<String, Object> consumerConfig) {
        return KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
                .withoutMetadata();
    }

    private static @UnknownKeyFor @NonNull @Initialized PTransform<@UnknownKeyFor @NonNull @Initialized PCollection<String>, @UnknownKeyFor @NonNull @Initialized PDone> kafkaWrite(IoTStreamingOptions options, String topic) {
        return KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopic(topic)
                .withValueSerializer(StringSerializer.class)
                .values();
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


        PCollection<InputDataNode0> node0 = pipeline
                .apply("Read from Kafka", kafkaRead(options, "input1", consumerConfig))
                .apply(Values.create())
                .apply(window(10, false, 0))
                .apply("Parse JSON to InputDataNode0", ParDo.of(new DoFn<String, InputDataNode0>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String jsonLine = c.element();
                        InputDataNode0 inputData = new Gson().fromJson(jsonLine, InputDataNode0.class);
                        c.output(inputData);
                    }
                }));


        PCollection<Double> node2 = node0
                .apply("Parse temperature", MapElements.into(TypeDescriptors.doubles())
                        .via((InputDataNode0 inputData) -> inputData.getTemperature().doubleValue()))
                .apply("Average temperature in Window", Mean.<Double>globally().withoutDefaults());

        PCollection<InputDataNode1> node1 = pipeline
                .apply("Read from Kafka", kafkaRead(options, "input2", consumerConfig))
                .apply(Values.create())
                .apply(window(10, false, 0))
                .apply("Parse JSON to InputDataNode1", ParDo.of(new DoFn<String, InputDataNode1>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String jsonLine = c.element();
                        InputDataNode1 inputData = new Gson().fromJson(jsonLine, InputDataNode1.class);
                        c.output(inputData);
                    }
                }));


        PCollection<Double> node3 = node1
                .apply("Parse temperature", MapElements.into(TypeDescriptors.doubles())
                        .via((InputDataNode1 inputData) -> inputData.getTemperature().doubleValue()))
                .apply("Average temperature in Window", Mean.<Double>globally().withoutDefaults());

        PCollection<Double> node4Join = PCollectionList.of(node2).and(node3)
                .apply(Flatten.<Double>pCollections());

        PCollection<String> outputPCollNode4 = node2.apply("Convert to JSON String", MapElements.into(TypeDescriptors.strings())
                .via((inputData) -> new Gson().toJson(inputData)));
        outputPCollNode4.apply("Write to File", TextIO.write().to("node4-output.txt").withWindowedWrites());

        PCollection<String> outputPCollNode5 = node3.apply("Convert to JSON String", MapElements.into(TypeDescriptors.strings())
                .via((inputData) -> new Gson().toJson(inputData)));
        outputPCollNode5.apply("Write to Kafka", kafkaWrite(options, "output"));

        // Run the pipeline
        pipeline.run();
    }
}