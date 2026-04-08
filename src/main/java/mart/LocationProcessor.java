package mart;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.GeoPoint;
import sales.dev.avro.LocationEvent;
import sales.dev.avro.SalesEvent;

public class LocationProcessor {

    public static Topology buildTopology(String schemaRegistryUrl) {

        StreamsBuilder builder = new StreamsBuilder();

        // Schema Registry config
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // SerDes
        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        SpecificAvroSerde<LocationEvent> locationSerde = new SpecificAvroSerde<>();
        locationSerde.configure(serdeConfig, false);

        // Source stream
        KStream<String, SalesEvent> sales =
                builder.stream(
                        "sales-test",
                        Consumed.with(Serdes.String(), salesSerde)
                )
                .peek((key, value) ->
                        System.out.println("RAW INPUT => key=" + key + ", value=" + value)
                );

        // Transformation (explicit typing fixes inference issues)
        KStream<String, LocationEvent> locationStream =
                sales.mapValues((ValueMapper<SalesEvent, LocationEvent>) sale -> {

                    LocationEvent locationEvent = LocationEvent.newBuilder()
                            .setLocation(
                                    (sale.getLatitude() != null && sale.getLongitude() != null)
                                            ? GeoPoint.newBuilder()
                                                .setLat(sale.getLatitude())
                                                .setLon(sale.getLongitude())
                                                .build()
                                            : null
                            )
                            .setTimestamp(sale.getTimestamp())
                            .build();

                    return locationEvent;
                })
                .peek((k, v) ->
                        System.out.println("LOCATION EVENT => " + v)
                );

        // Sink topic
        locationStream.to(
                "location-events",
                Produced.with(Serdes.String(), locationSerde)
        );

        return builder.build();
    }

    public static void main(String[] args) {

        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kmart-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.ByteArray().getClass().getName());

        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);

        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.AT_LEAST_ONCE);

        Topology topology = buildTopology(schemaRegistryUrl);

        System.out.println("Topology: " + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable exception) {
                System.err.println("Stream error: " + exception.getMessage());
                exception.printStackTrace();
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        });

        streams.setStateListener((newState, oldState) ->
                System.out.println("State changed: " + oldState + " → " + newState)
        );

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("Location Streams App started...");
    }
}