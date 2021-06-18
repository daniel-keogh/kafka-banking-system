import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Application {
    private static final String VALID_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        String consumerGroup = "reporting-service";

        System.out.println("Consumer is part of consumer group " + consumerGroup);

        Consumer<String, Transaction> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        consumeMessages(Arrays.asList(VALID_TOPIC, SUSPICIOUS_TOPIC), kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, Transaction> records = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (var record : records) {
                System.out.println(String.format("Received record with (key %s, value %s, partition %s, offset %s)",
                        record.key(), record.value(), record.partition(), record.offset()));

                recordTransactionForReporting(record.topic(), record.value());
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

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print a different message depending on whether transaction is suspicious or valid
        if (topic.equalsIgnoreCase(VALID_TOPIC)) {
            System.out.printf("Recording transaction for user %s, amount $%s to show it on user's monthly statement\n",
                    transaction.getUser(), transaction.getAmount());
        } else if (topic.equalsIgnoreCase(SUSPICIOUS_TOPIC)) {
            System.out.printf("Recording suspicious transaction for user %s, amount of $%s originating in %s for further investigation\n",
                    transaction.getUser(), transaction.getAmount(), transaction.getTransactionLocation());
        }
    }
}
