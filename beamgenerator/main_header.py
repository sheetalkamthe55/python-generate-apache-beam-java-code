from .utility import create_input_schema_classes

def create_import_header(package_name):
    ## Imports and utility for pipeline ## # TODO: maybe move to separate file (also create schema classes in separate files)
    return f'''
package {package_name};

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

import {package_name}.InputData;
import com.google.gson.Gson;
import org.apache.beam.sdk.values.TypeDescriptors;

    '''

def create_main_helper_classes(bootstrap_server_default = "localhost:9092"):
    return f'''
    private static final Logger LOG = LogManager.getLogger(TestBeamPipeline.class);

    public interface IoTStreamingOptions extends PipelineOptions {{

        @Description("Kafka bootstrap servers, comma separated list")
        @Default.String({bootstrap_server_default})
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
    }}

    private static PTransform<@UnknownKeyFor @NonNull @Initialized PBegin, @UnknownKeyFor @NonNull @Initialized PCollection<@UnknownKeyFor @NonNull @Initialized KV<String, String>>> kafkaRead(IoTStreamingOptions options, String test, Map<String, Object> consumerConfig) {{
        return KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaBootstrapServers())
                .withTopicPartitions(
                        Collections.singletonList(new TopicPartition(test, 0))
                )
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(consumerConfig) // TODO: needed?
                .withoutMetadata();
    }}

    private static <T> Window<T> window(long duration, boolean sliding, long period) {{
        Window<T> window;
        if (sliding) {{
            window = Window.<T>into(SlidingWindows.of(Duration.standardSeconds(duration)).every(Duration.standardSeconds(period)));
        }} else {{
            window = Window.<T>into(FixedWindows.of(Duration.standardSeconds(duration)));
        }}
        return window.triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1))))
            .withAllowedLateness(Duration.standardSeconds(5))
            .discardingFiredPanes();
    }}

    public static void main(String[] args) {{
        // Create the pipeline options
        IoTStreamingOptions options = PipelineOptionsFactory.fromArgs(args).as(IoTStreamingOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put("auto.offset.reset", "earliest");

'''

def create_main_header(package_name, source_node_map, bootstrap_server_default = "localhost:9092"):
    ## Create top of the main class ##
    beam_main = create_import_header(package_name)
    beam_main += '''
    public class TestBeamPipeline {

    '''
    beam_main += create_input_schema_classes(source_node_map)
    # streaming options interface, kafka read transform, window transform and main method header
    beam_main += create_main_helper_classes(bootstrap_server_default)
    return beam_main