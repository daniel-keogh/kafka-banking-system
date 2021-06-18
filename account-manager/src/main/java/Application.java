import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Application {
    private static final String VALID_TOPIC = "valid-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "account-manager";

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(VALID_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<String, Transaction> records = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (var record : records) {
                System.out.printf("Received record (key %s, value %s, partition %s, offset %s)%n",
                        record.key(), record.value(), record.partition(), record.offset());

                approveTransaction(record.value());
            }

            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void approveTransaction(Transaction transaction) {
        System.out.printf("Authorizing transaction for user %s, in the amount of $%s\n",
                transaction.getUser(), transaction.getAmount());
    }
}
