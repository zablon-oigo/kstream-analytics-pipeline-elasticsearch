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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.CustomerSpend;
import sales.dev.avro.RewardEvent;
import sales.dev.avro.SalesEvent;

public class RewardStream {

    public static void build(StreamsBuilder builder, String schemaRegistryUrl) {

        // Schema Registry configuration

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);

      
        // Avro Serdes

        final SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<RewardEvent> rewardSerde = new SpecificAvroSerde<>();
        rewardSerde.configure(serdeConfig, false);

        final SpecificAvroSerde<CustomerSpend> spendSerde = new SpecificAvroSerde<>();
        spendSerde.configure(serdeConfig, false);


        // Read sales events

        KStream<String, SalesEvent> salesStream = builder.stream(
                "sales-test", 
                Consumed.with(Serdes.String(), salesSerde)
        );

        //  Aggregate spend per customer

        KTable<String, CustomerSpend> customerSpend = salesStream
                .selectKey((key, value) -> value.getCustomerId().toString())
                .groupByKey(Grouped.with(Serdes.String(), salesSerde))
                .aggregate(
                        () -> {
                            CustomerSpend init = new CustomerSpend();
                            init.setEmail("");
                            init.setTotalSpend(0.0);
                            return init;
                        },
                        (customerId, sale, total) -> {

                            double newTotal = total.getTotalSpend() +
                                    (sale.getPrice() * sale.getQuantity());

                            String email = sale.getEmail() != null
                                    ? sale.getEmail().toString()
                                    : total.getEmail();

                            CustomerSpend result = new CustomerSpend();
                            result.setEmail(email);
                            result.setTotalSpend(newTotal);

                            return result;
                        },
                        Materialized.with(Serdes.String(), spendSerde)
                );


        // State store for deduplication

        StoreBuilder<KeyValueStore<String, Boolean>> rewardStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("reward-store"),
                        Serdes.String(),
                        Serdes.Boolean()
                );

        builder.addStateStore(rewardStore);

        //  Apply reward logic

        KStream<String, RewardEvent> rewards = customerSpend.toStream()
                .processValues(
                        () -> new RewardTransformer(),
                        "reward-store"
                );

        // Send to Kafka topic

        rewards.to("rewards-events",
                Produced.with(Serdes.String(), rewardSerde)
        );
    }
}