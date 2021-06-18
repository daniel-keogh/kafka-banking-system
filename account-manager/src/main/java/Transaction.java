import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class Transaction {
    private String user;
    private double amount;
    private String transactionLocation;

    public String getUser() {
        return user;
    }

    public double getAmount() {
        return amount;
    }

    public String getTransactionLocation() {
        return transactionLocation;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setTransactionLocation(String transactionLocation) {
        this.transactionLocation = transactionLocation;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "user='" + user + '\'' +
                ", amount=" + amount +
                ", transactionLocation='" + transactionLocation + '\'' +
                '}';
    }

    /**
     * Kafka Deserializer implementation.
     * Deserializes a Transaction from JSON to a {@link Transaction} object
     */
    public static class TransactionDeserializer implements Deserializer<Transaction> {

        @Override
        public Transaction deserialize(String topic, byte[] data) {
            ObjectMapper mapper = new ObjectMapper();
            Transaction transaction = null;
            try {
                transaction = mapper.readValue(data, Transaction.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return transaction;

        }
    }
}
