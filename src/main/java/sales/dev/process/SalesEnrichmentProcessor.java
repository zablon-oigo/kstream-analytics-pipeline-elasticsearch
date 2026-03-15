package sales.dev.process;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import sales.dev.avro.CustomerEvent;
import sales.dev.avro.SalesEvent;

public class SalesEnrichmentProcessor {

    public static KStream<String, SalesEvent> enrichSales(
            KStream<String, SalesEvent> sales,
            KTable<String, CustomerEvent> customers,
            Map<String, String> serdeConfig
    ) {

        SpecificAvroSerde<SalesEvent> salesSerde = new SpecificAvroSerde<>();
        salesSerde.configure(serdeConfig, false);

        SpecificAvroSerde<CustomerEvent> customerSerde = new SpecificAvroSerde<>();
        customerSerde.configure(serdeConfig, false);

        KStream<String, SalesEvent> maskedSales = sales.mapValues(sale -> {

            String card = sale.getCardNumber();

            if (card != null && card.length() > 4) {
                String masked = "XXXX-XXXX-XXXX-" +
                        card.substring(card.length() - 4);
                sale.setCardNumber(masked);
            }

            return sale;
        });
    }
}
