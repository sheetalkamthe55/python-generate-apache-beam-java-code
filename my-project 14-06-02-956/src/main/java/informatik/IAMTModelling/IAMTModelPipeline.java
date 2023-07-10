package informatik.IAMTModelling;

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
import informatik.IAMTModelling.SensorData;
import org.apache.beam.sdk.values.TypeDescriptors;


import com.google.gson.Gson;

public class IAMTModelPipeline {
    public static void main(String[] args) {
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        runIAMTModel(options);
    }

static void runIAMTModel(PipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    final Map<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("auto.offset.reset", "earliest");

    p.apply(KafkaIO.<Long, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopicPartitions(Collections.singletonList(new TopicPartition("echo-input", 0)))
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withConsumerConfigUpdates(consumerConfig)
            .withMaxNumRecords(10)
            .withoutMetadata())
     .apply(Values.create())
.apply("Parse JSON to SensorData", ParDo.of(new DoFn<String, SensorData>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String jsonLine = c.element();
        SensorData sensordata = new Gson().fromJson(jsonLine, SensorData.class);
        System.out.println("Processing element: " + sensordata.getComponent() + "," + sensordata.getId() + "," + sensordata.getTemperature());
        c.output(sensordata);
    }}))
.apply("Filter by Temperature", Filter.by((SensorData sensordata) -> sensordata.getTemperature() > 25.0))
.apply("Format SensorData to CSV", MapElements
                .into(TypeDescriptors.strings())
                .via((SensorData sensordata) ->
                sensordata.getComponent() + "," + sensordata.getId() + "," + sensordata.getTemperature()))
.apply(TextIO.write().to("IAMTModel"));
p.run().waitUntilFinish();}
}