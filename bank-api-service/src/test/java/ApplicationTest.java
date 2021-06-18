import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ApplicationTest {

    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private IncomingTransactionsReader transactionsReader;
    private CustomerAddressDatabase userDb;
    private MockProducer mockProducer;

    @BeforeEach
    private void setup() {
        transactionsReader = new IncomingTransactionsReader("test-transactions.txt");
        userDb = new CustomerAddressDatabase("test-user-residence.txt");
        mockProducer = new MockProducer<>(true, new StringSerializer(), new Transaction.TransactionSerializer());
    }


    @Test
    void testProducesMessages() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        assertEquals(5, mockProducer.history().size());
    }


    @Test
    public void testValidTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<String, Transaction> record =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(0);

        assertEquals(VALID_TRANSACTIONS_TOPIC, record.topic());
    }


    @Test
    public void testSuspiciousTransactionsTopic() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);

        ProducerRecord<Long, String> record = (ProducerRecord<Long, String>) mockProducer.history().get(1);

        assertEquals(SUSPICIOUS_TRANSACTIONS_TOPIC, record.topic());
    }

    @Test
    public void testMessageContents() throws ExecutionException, InterruptedException {
        Application testApp = new Application();
        testApp.processTransactions(transactionsReader, userDb, mockProducer);
        String testUser = "joe1680";
        String testLocation = "Ireland";
        double testAmount = 128.63;

        Transaction expectedTransaction = new Transaction(testUser, testAmount, testLocation);

        ProducerRecord<String, Transaction> record  =
                (ProducerRecord<String, Transaction>) mockProducer.history().get(0);

        assertEquals(expectedTransaction.getUser(), record.key());
        assertEquals(expectedTransaction, record.value());
    }
}