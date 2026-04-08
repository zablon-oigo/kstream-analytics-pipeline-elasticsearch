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
import sales.dev.avro.SalesEvent;
import sales.dev.avro.TransactionEvent;

public class TransactionStream {

    public static void build(StreamsBuilder builder, String schemaRegistryUrl) {

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(config, false);

        SpecificAvroSerde<TransactionEvent> transactionSerde = new SpecificAvroSerde<>();
        transactionSerde.configure(config, false);

        KStream<String, SalesEvent> sales =
                builder.stream("sales-test",
                        Consumed.with(Serdes.String(), salesSerde));

        KStream<String, TransactionEvent> transactions =
                sales.mapValues(sale -> {

                    long ts = sale.getTimestamp() != null
                            ? sale.getTimestamp().toEpochMilli()
                            : System.currentTimeMillis();

                    return TransactionEvent.newBuilder()
                            .setOrderId(safe(sale.getOrderId()))
                            .setCustomerId(safe(sale.getCustomerId()))
                            .setProductId(safe(sale.getProductId()))
                            .setProductName(safe(sale.getProductName()))
                            .setQuantity(sale.getQuantity())
                            .setPrice(sale.getPrice())
                            .setTimestamp(ts)
                            .setCardMasked(mask(safe(sale.getCardNumber())))
                            .build();
                });

        transactions
                .peek((k, v) ->
                        System.out.println("TRANSACTION:  " + v)
                )
        .to("transactions-events",
                Produced.with(Serdes.String(), transactionSerde));
    }

    private static String safe(Object v) {
        return v != null ? v.toString() : null;
    }

    private static String mask(String card) {
        if (card == null || card.length() < 4) return "****";
        return "****-****-****-" + card.substring(card.length() - 4);
    }
}