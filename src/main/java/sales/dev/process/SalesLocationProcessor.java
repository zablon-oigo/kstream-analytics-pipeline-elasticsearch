package sales.dev.process;

import org.apache.kafka.streams.kstream.KStream;

import sales.dev.avro.SalesEvent;

public class SalesLocationProcessor {

    public static KStream<String, SalesEvent> extractLocation(
            KStream<String, SalesEvent> enrichedSales
    ) {
        return enrichedSales.mapValues(sale -> {
            String geo = sale.getLatitude() + "," + sale.getLongitude();
            return sale;
        });
    }
}
