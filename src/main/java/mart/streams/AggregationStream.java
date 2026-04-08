package mart.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.CountryCount;
import sales.dev.avro.CustomerEvent;
import sales.dev.avro.SalesEvent;

public class AggregationStream {

    public static void build(StreamsBuilder builder, String schemaRegistryUrl) {

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(config, false);

        SpecificAvroSerde<CustomerEvent> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(config, false);

        SpecificAvroSerde<CountryCount> countryCountSerde = new SpecificAvroSerde<>();
        countryCountSerde.configure(config, false);

        KStream<String, SalesEvent> sales =
                builder.stream("sales-test", //Topic
                        Consumed.with(Serdes.String(), salesSerde));

        KStream<String, CustomerEvent> customers =
                sales.mapValues(sale ->
                        CustomerEvent.newBuilder()
                                .setCustomerId(safe(sale.getCustomerId()))
                                .setCountry(safe(sale.getCountry()))
                                .build()
                );

        KTable<String, Long> countByCountry =
                customers
                        .filter((k, v) -> v.getCountry() != null)
                        .selectKey((k, v) -> v.getCountry())
                        .groupByKey(Grouped.with(Serdes.String(), customerSerde))
                        .count(Materialized.as("country-store"));

        // countByCountry.toStream()
        //         .peek((k, v) ->
        //             System.out.println("AGG:  " + k + " = " + v))
        //         .to("customer-count-by-country",
        //                 Produced.with(Serdes.String(), Serdes.Long()));

        countByCountry.toStream()
        .map((String country, Long count) ->
                org.apache.kafka.streams.KeyValue.pair(
                        country,
                        CountryCount.newBuilder()
                                .setCountry(country)
                                .setCount(count)
                                .build()
                )
        )
        .peek((k, v) -> System.out.println("AGG:  " + k + " = " + v))
        .to("customer-count-by-country",
                Produced.with(Serdes.String(), countryCountSerde));
    }

    private static String safe(Object v) {
        return v != null ? v.toString() : null;
    }
}