package mart;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import mart.streams.AggregationStream;
import mart.streams.CustomerStream;
import mart.streams.RewardStream;
import mart.streams.TransactionStream;

public class SalesProcessor {

    public static void main(String[] args) {

        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put("application.id", "sales-processor");
        props.put("bootstrap.servers", "localhost:9095");
        props.put("schema.registry.url", schemaRegistryUrl);

        // ✅ Create StreamsBuilder (NOT Topology directly)
        StreamsBuilder builder = new StreamsBuilder();

        // ✅ Pass builder to modules
        CustomerStream.build(builder, schemaRegistryUrl);
        TransactionStream.build(builder, schemaRegistryUrl);
        AggregationStream.build(builder, schemaRegistryUrl);
        RewardStream.build(builder, schemaRegistryUrl);

        // ✅ Build topology AFTER wiring everything
        Topology topology = builder.build();

        System.out.println("TOPOLOGY:");
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("✅ Application started");
    }
}