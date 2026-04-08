package mart;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.datafaker.Faker;
import sales.dev.avro.SalesEvent;

public class App {

    private static final String BOOTSTRAP = "localhost:9095,localhost:9102,localhost:9097";
    private static final String SCHEMA_REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "sales-test";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY);

        KafkaProducer<String, SalesEvent> producer = new KafkaProducer<>(props);

        Faker faker = new Faker();

        try {
            while (true) {

                // Create event
                SalesEvent order = new SalesEvent();

                String orderId = UUID.randomUUID().toString();
                String customerId = UUID.randomUUID().toString();
                String productId = UUID.randomUUID().toString();

                long timestamp = System.currentTimeMillis();

                String productName = faker.commerce().productName();
                String category = faker.commerce().department();

                int quantity = ThreadLocalRandom.current().nextInt(1, 5);
                double price = Double.parseDouble(faker.commerce().price());

                String city = faker.address().city();
                String country = faker.address().country();
                double latitude = Double.parseDouble(faker.address().latitude());
                double longitude = Double.parseDouble(faker.address().longitude());

                String firstName = faker.name().firstName();
                String lastName = faker.name().lastName();

                String email = faker.internet().emailAddress();

                String cardNumber = faker.finance().creditCard();

                // Populate Avro object
                order.setOrderId(orderId);
                order.setCustomerId(customerId);
                order.setProductId(productId);
                order.setTimestamp(Instant.ofEpochMilli(timestamp));
                order.setProductName(productName);
                order.setCategory(category);
                order.setQuantity(quantity);
                order.setPrice(price);
                order.setCountry(country);
                order.setCity(city);
                order.setLatitude(latitude);
                order.setLongitude(longitude);
                order.setFirstName(firstName);
                order.setLastName(lastName);
                order.setCardNumber(cardNumber);
                order.setEmail(email);

                // Send to Kafka
                ProducerRecord<String, SalesEvent> record =
                        new ProducerRecord<>(TOPIC, customerId, order);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent to topic=" + metadata.topic() +
                                " partition=" + metadata.partition() +
                                " offset=" + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}