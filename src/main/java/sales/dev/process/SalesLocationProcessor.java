package sales.dev.process;

import org.apache.kafka.streams.kstream.KStream;
import sales.dev.avro.LocationEvent;
import sales.dev.avro.SalesEvent;

public class SalesLocationProcessor {

    public static KStream<String, LocationEvent> extractLocation(
            KStream<String, SalesEvent> enrichedSales
    ) {
        return enrichedSales.mapValues(sale -> {
            LocationEvent location = new LocationEvent();
            location.setLatitude(sale.getLatitude());
            location.setLongitude(sale.getLongitude());
            return location;
        });
    }
}