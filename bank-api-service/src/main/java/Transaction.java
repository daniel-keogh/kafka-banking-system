import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Objects;

public class Transaction {
    private String user;
    private double amount;
    private String transactionLocation;

    public Transaction(String user, double amount, String transactionLocation) {
        this.user = user;
        this.amount = amount;
        this.transactionLocation = transactionLocation;
    }

    public String getUser() {
        return user;
    }

    public double getAmount() {
        return amount;
    }

    public String getTransactionLocation() {
        return transactionLocation;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "user='" + user + '\'' +
                ", amount=" + amount +
                ", transactionLocation='" + transactionLocation + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Double.compare(that.amount, amount) == 0 &&
                user.equals(that.user) &&
                transactionLocation.equals(that.transactionLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, amount, transactionLocation);
    }

    /**
     * Kafka Serializer implementation.
     * Serializes a Transaction to JSON so it can be sent to a Kafka Topic
     */
    public static class TransactionSerializer implements Serializer<Transaction> {
        @Override
        public byte[] serialize(String topic, Transaction data) {
            byte[] serializedData = null;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                serializedData = objectMapper.writeValueAsString(data).getBytes();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return serializedData;
        }
    }
}
