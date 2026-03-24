package sales.dev.process;

import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

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

        // Step 1: Mask card 
        KStream<String, SalesEvent> maskedSales = sales.mapValues(sale -> {

            SalesEvent maskedSale = new SalesEvent();

            // copy fields
            maskedSale.setCustomerId(sale.getCustomerId());
            maskedSale.setProduct(sale.getProduct());
            maskedSale.setAmount(sale.getAmount());
            maskedSale.setLatitude(sale.getLatitude());
            maskedSale.setLongitude(sale.getLongitude());

            // mask card
            String card = sale.getCardNumber();
            if (card != null && card.length() > 4) {
                maskedSale.setCardNumber(
                        "XXXX-XXXX-XXXX-" + card.substring(card.length() - 4)
                );
            } else {
                maskedSale.setCardNumber(card);
            }

            return maskedSale;
        });

        // Step 2: Re-key by customerId
        KStream<String, SalesEvent> salesByCustomer =
                maskedSales.selectKey((key, sale) -> sale.getCustomerId());

        // Step 3: Join and enrich
        return salesByCustomer.join(
                customers,
                (sale, customer) -> {

                    SalesEvent enriched = new SalesEvent();

                    // copy sales data
                    enriched.setCustomerId(sale.getCustomerId());
                    enriched.setProduct(sale.getProduct());
                    enriched.setAmount(sale.getAmount());
                    enriched.setLatitude(sale.getLatitude());
                    enriched.setLongitude(sale.getLongitude());
                    enriched.setCardNumber(sale.getCardNumber());

                    // add customer data
                    if (customer != null) {
                        enriched.setFirstName(customer.getFirstName());
                        enriched.setLastName(customer.getLastName());
                    }

                    return enriched;
                },
                Joined.with(
                        Serdes.String(),
                        salesSerde,
                        customerSerde
                )
        );
    }
}