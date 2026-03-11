package sales.dev;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import net.datafaker.Faker;

public class SalesProducer {

    private static final String BOOTSTRAP = "localhost:9095,localhost:9102,localhost:9097";
    private static final String SCHEMA_REGISTRY = "http://localhost:8081";

    public static void main(String[] args) throws Exception {

        // Faker 
        Faker faker = new Faker(new Locale("en")); 

        // Kenyan cities with lat/lon
        Map<String, double[]> kenyaCities = Map.ofEntries(
            Map.entry("Nairobi",   new double[]{-1.2921, 36.8219}),
            Map.entry("Mombasa",   new double[]{-4.0435, 39.6682}),
            Map.entry("Kisumu",    new double[]{-0.0917, 34.7679}),
            Map.entry("Nakuru",    new double[]{-0.3031, 36.0800}),
            Map.entry("Eldoret",   new double[]{ 0.5143, 35.2696}),
            Map.entry("Thika",     new double[]{-1.0331, 37.0699}),
            Map.entry("Kitale",    new double[]{ 1.0150, 35.0065}),
            Map.entry("Nyeri",     new double[]{-0.4167, 36.9500}),
            Map.entry("Naivasha",  new double[]{-0.7167, 36.4333}),
            Map.entry("Machakos",  new double[]{-1.5167, 37.2667}),
            Map.entry("Malindi",   new double[]{-3.2167, 40.1167}),
            Map.entry("Garissa",   new double[]{-0.4546, 39.6583}),
            Map.entry("Wajir",     new double[]{ 1.7483, 40.0586}),
            Map.entry("Kakamega",  new double[]{ 0.2823, 34.7519}),
            Map.entry("Bungoma",   new double[]{ 0.5690, 34.5600}),
            Map.entry("Meru",      new double[]{ 0.0500, 37.6500}),
            Map.entry("Kisii",     new double[]{-0.6817, 34.7667}),
            Map.entry("Nyamira",   new double[]{-0.5633, 34.9358})
        );

         // Product categories and products
        Map<String, List<String>> categoryProducts = Map.ofEntries(
            Map.entry("Electronics", List.of("Laptop", "Smartphone", "Headphones", "Tablet", "Monitor")),
            Map.entry("Clothing", List.of("T-Shirt", "Jeans", "Jacket", "Sneakers", "Dress")),
            Map.entry("Books", List.of("Novel", "Textbook", "Comic Book", "Biography", "Cookbook")),
            Map.entry("Home & Kitchen", List.of("Blender", "Coffee Maker", "Pillow", "Towel Set", "Knife Set"))
        );

        List<String> cities = new ArrayList<>(kenyaCities.keySet());

        // Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);


        try (KafkaProducer<String, SpecificRecord> producer = new KafkaProducer<>(props)) {

            // Generate Kenyan customers
            int numCustomers = 80;
            List<String> generatedCustomerIds = new ArrayList<>();

            for (int i = 0; i < numCustomers; i++) {
                String customerId = "CUST-" + String.format("%05d", i + 1);
                generatedCustomerIds.add(customerId);

                String city = cities.get(faker.number().numberBetween(0, cities.size()));
                double[] latLon = kenyaCities.get(city);

                CustomerEvent cust = new CustomerEvent();
                cust.setCustomerId(customerId);
                cust.setFirstName(faker.name().firstName());
                cust.setLastName(faker.name().lastName());
                cust.setEmail(faker.internet().safeEmailAddress());
                cust.setCountry("Kenya");
                cust.setCity(city);
                cust.setLatitude(latLon[0]);
                cust.setLongitude(latLon[1]);
                cust.setCardNumber(faker.finance().creditCard().replaceAll("\\D", ""));
                cust.setTimestamp(Instant.now());

                producer.send(new ProducerRecord<>("customer-profile", customerId, cust)).get();
                System.out.println("Created customer: " + customerId + " in " + city);
                TimeUnit.MILLISECONDS.sleep(150);
            }
            System.out.println("All customers created. Starting sales orders...");
        }
    }

}