import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    private static final String VALID_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TOPIC = "suspicious-transactions";
    private static final String HIGH_VALUE_TOPIC = "high-value-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final double HIGH_VALUE_AMOUNT = 1000;

    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();

        Application app = new Application();

        Producer<String, Transaction> kafkaProducer = app.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            app.processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        // Retrieve the next transaction from the IncomingTransactionsReader
        while (incomingTransactionsReader.hasNext()) {
            Transaction next = incomingTransactionsReader.next();

            // Check if the transaction is high-value and if so, send it to the relevant topic
            if (next.getAmount() > HIGH_VALUE_AMOUNT) {
                var record = new ProducerRecord<>(HIGH_VALUE_TOPIC, next.getUser(), next);
                kafkaProducer.send(record).get();
            }

            // Compare user residence to transaction location and send a message to the appropriate topic,
            // depending on whether the user residence and transaction location match or not.
            String residence = customerAddressDatabase.getUserResidence(next.getUser());

            ProducerRecord<String, Transaction> record;
            if (residence.equalsIgnoreCase(next.getTransactionLocation())) {
                record = new ProducerRecord<>(VALID_TOPIC, next.getUser(), next);
            } else {
                record = new ProducerRecord<>(SUSPICIOUS_TOPIC, next.getUser(), next);
            }

            RecordMetadata metadata = kafkaProducer.send(record).get();

            // Print record metadata information
            System.out.printf("Record with (key %s, value %s) was sent to (partition %s, offset %s, topic %s)%n",
                    record.key(), record.value(), metadata.partition(), metadata.offset(), metadata.topic());
        }
    }

    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transactions-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}
