package mart.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.CustomerEvent;
import sales.dev.avro.SalesEvent;

public class CustomerStream {

    public static void build(StreamsBuilder builder, String schemaRegistryUrl) {

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(config, false);

        SpecificAvroSerde<CustomerEvent> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(config, false);

        KStream<String, SalesEvent> sales =
                builder.stream("sales-test", 
                        Consumed.with(Serdes.String(), salesSerde));

        KStream<String, CustomerEvent> customers =
                sales.mapValues(sale ->
                        CustomerEvent.newBuilder()
                                .setCustomerId(safe(sale.getCustomerId()))
                                .setEmail(safe(sale.getEmail()))
                                .setFirstName(safe(sale.getFirstName()))
                                .setLastName(safe(sale.getLastName()))
                                .setCountry(safe(sale.getCountry()))
                                .setCity(safe(sale.getCity()))
                                .build()
                )
                .peek((k, v) ->
                        System.out.println("CUSTOMER:  " + v)
                );

        customers
        .to("customer-events",
                Produced.with(Serdes.String(), customerSerde));
    }

    private static String safe(Object v) {
        return v != null ? v.toString() : null;
    }
}