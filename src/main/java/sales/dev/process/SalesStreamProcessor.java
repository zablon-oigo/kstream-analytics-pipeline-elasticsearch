package sales.dev.process;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.CustomerEvent;
import sales.dev.avro.SalesEvent;

public class SalesStreamProcessor {

    public static Topology buildTopology(String schemaRegistryUrl) {
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> serdeConfig = Map.of("schema.registry.url", schemaRegistryUrl);

        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        SpecificAvroSerde<CustomerEvent> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);
        
        // Source topic

        KStream<String, SalesEvent> sales = builder.stream(
                "sales-raw",
                Consumed.with(Serdes.String(), salesSerde)
        );

        KTable<String, CustomerEvent> customers = builder.table(
                "customer-profile",
                Consumed.with(Serdes.String(), customerSerde)
        );

    }

}