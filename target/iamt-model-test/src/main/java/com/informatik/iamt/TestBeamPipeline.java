package com.informatik.iamt;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.StringSerializer;
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


import org.apache.kafka.common.serialization.StringDeserializer;

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

    private static PTransform<@UnknownKeyFor @NonNull @Initialized PBegin, @UnknownKeyFor @NonNull @Initialized PCollection<@UnknownKeyFor @NonNull @Initialized KV<String, String>>> kafkaRead(IoTStreamingOptions options, String topic, Map<String, Object> consumerConfig) {
        return KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata();
    }

    private static @UnknownKeyFor @NonNull @Initialized PTransform<@UnknownKeyFor @NonNull @Initialized PCollection<String>, @UnknownKeyFor @NonNull @Initialized PDone> kafkaWrite(IoTStreamingOptions options, String topic) {
        return KafkaIO.<String, String>write()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopic("output")
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
        consumerConfig.put("auto.offset.reset", "earliest"); // TODO: make configurable

        // Read from Kafka
        PCollection<InputData> input1 = pipeline
                .apply("Read from Kafka", kafkaRead(options, "test1", consumerConfig))
                .apply(Values.create())
                .apply(window(30, true, 15))
                .apply("Parse JSON to InputData", ParDo.of(new DoFn<String, InputData>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String jsonLine = c.element();
                        InputData inputdata = new Gson().fromJson(jsonLine, InputData.class);
                        c.output(inputdata);
                    }
                }));
        PCollection<InputData> input2 = pipeline
                .apply("Read from Kafka", kafkaRead(options, "test2", consumerConfig))
                .apply(Values.create())
                .apply(window(30, true, 15))
                .apply("Parse JSON to InputData", ParDo.of(new DoFn<String, InputData>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String jsonLine = c.element();
                        InputData inputdata = new Gson().fromJson(jsonLine, InputData.class);
                        c.output(inputdata);
                    }
                }));

        // example of creating KV collection <String, Double> from both input collections, merging, and calculating average per key
        PCollection<KV<String, Double>> test1KV = input1
                .apply("Create KV Component/Temp", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((InputData inputData) -> KV.of(inputData.getComponent(), inputData.getTemperature().doubleValue())));
        PCollection<KV<String, Double>> test2KV = input2
                .apply("Create KV Component/Temp", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                        .via((InputData inputData) -> KV.of(inputData.getComponent(), inputData.getTemperature().doubleValue())));
        PCollection<KV<String, Double>> mergedCollDifferentVersion = PCollectionList.of(test1KV).and(test2KV)
                .apply(Flatten.<KV<String, Double>>pCollections());
        PCollection<KV<String, Double>> avgDifferentVersion = mergedCollDifferentVersion.apply("Average Temperature per Component", Mean.perKey());
        PCollection<String> testAvgCollDifferentVersion = avgDifferentVersion.apply("Format Output", MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Double> kv) -> {
                    return kv.getKey() + ": " + kv.getValue().toString();

                }));

        // example of first merging and then creating output collection
        PCollection<InputData> mergedColl = PCollectionList.of(input1).and(input2)
                .apply(Flatten.<InputData>pCollections());

        // filter test1 collection
        // PCollection<InputData> test1Coll = inputDataColl.apply("Filter test1", Filter.by((InputData inputData) -> inputData.getComponent().equals("test1")));

        // average temperature per component
        PCollection<KV<String, InputData>> testKV = mergedColl
            .apply("Create KV", WithKeys.of(
                    new SerializableFunction<InputData, String>() {
                        @Override
                        public String apply(InputData s) {
                            return s.getComponent();
                        }
                    }));
        PCollection<KV<String, Double>> tempFirst = testKV.apply("Map to Temperature", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via((KV<String, InputData> kv) -> KV.of(kv.getKey(), kv.getValue().getTemperature().doubleValue())));
        PCollection<KV<String, Double>> avgFirst = tempFirst.apply("Average Temperature per Component", Mean.perKey());
            // format output to string
        PCollection<String> testAvgCollFirstVariant = avgFirst
            // .apply("Format Output", MapElements.into(TypeDescriptors.strings())
            //     .via((KV<String, Double> kv) -> {
            //         return kv.getKey() + ": " + kv.getValue().toString();
            //     }));
            .apply("Convert to JSON", MapElements.into(TypeDescriptors.strings())
                .via((inputData) -> new Gson().toJson(inputData)));

        // average temperature per component
        PCollection<String> testAvgCollSecondVariant = mergedColl
                .apply("Create KV", WithKeys.of(
                        new SerializableFunction<InputData, String>() {
                            @Override
                            public String apply(InputData s) {
                                return s.getComponent();
                            }
                        }))
                .apply("Group by key", GroupByKey.create())
                // iterate over all values per key
                .apply("Iterate over values", ParDo.of(new DoFn<KV<String, Iterable<InputData>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow window) {
                        String component = c.element().getKey();
                        Iterable<InputData> inputDataIterable = c.element().getValue();
                        Double sum = 0.0;
                        int count = 0;
                        for (InputData inputData : inputDataIterable) {
                            sum += inputData.getTemperature();
                            count++;
                        }
                        Double avg = sum / count;
                        //System.out.println("[" + window.maxTimestamp().toString() + "] " + component + ": " + avg.toString() + " (" + count + " values)");
                        c.output(component + ": " + avg.toString());
                    }
                }))
                .apply("Convert to JSON", MapElements.into(TypeDescriptors.strings())
                    .via((inputData) -> new Gson().toJson(inputData)));

        // average temperature per component

        PCollection<String> globalAvgColl = mergedColl
                .apply("Parse Temperature", MapElements.into(TypeDescriptors.doubles())
                    .via((InputData inputData) -> inputData.getTemperature().doubleValue()))
                .apply("Average Temperature in Window", Mean.<Double>globally().withoutDefaults())
                // .apply("Format Output", MapElements.into(TypeDescriptors.strings())
                    // .via(d -> Double.toString(d)));
                .apply("Convert to JSON", MapElements.into(TypeDescriptors.strings())
                    .via((inputData) -> new Gson().toJson(inputData)));

        globalAvgColl.apply("Write to File", TextIO.write().to(options.getOutputFile()).withWindowedWrites());
        testAvgCollFirstVariant.apply("Write to File", TextIO.write().to(options.getOutputFile()).withWindowedWrites());
        testAvgCollSecondVariant.apply("Write to File", TextIO.write().to(options.getOutputFile()).withWindowedWrites());


        globalAvgColl.apply("Write to Kafka", kafkaWrite(options, "output"));
        globalAvgColl.apply("Write to Console", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        // TODO: write output to delta lake
        // use delta lake standalone https://docs.delta.io/latest/delta-standalone.html

        // Run the pipeline
        pipeline.run();
    }
}