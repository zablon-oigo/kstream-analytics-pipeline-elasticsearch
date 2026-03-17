package sales.dev.process;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
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

        KStream<String, SalesEvent> enrichedSales = SalesEnrichmentProcessor.enrichSales(
                sales, customers, serdeConfig
        );

        enrichedSales.to(
                "sales-enriched",
                Produced.with(Serdes.String(), salesSerde)
        );
        // Extract location
        KStream<String, SalesEvent> salesLocation = SalesLocationProcessor.extractLocation(enrichedSales);
        salesLocation.to(
                "location",
                Produced.with(Serdes.String(), salesSerde)
        );

        return builder.build();

    }
        public static void main(String[] args) {
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9095,localhost:9102,localhost:9097");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);


        }

}