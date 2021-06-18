import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Mock database that contains a map from a user to its country of residence
 */
public class CustomerAddressDatabase {
    private static final String DEFAULT_USER_RESIDENCE_FILE = "user-residence.txt";
    private final Map<String, String> userToResidenceMap;
    private final String userResidenceFile;

    public CustomerAddressDatabase(){
        this(DEFAULT_USER_RESIDENCE_FILE);
    }

    public CustomerAddressDatabase(String userResidenceFile) {
        this.userResidenceFile = userResidenceFile;
        this.userToResidenceMap = loadUsersResidenceFromFile();
    }

    /**
     * Returns the user's country of residence
     */
    public String getUserResidence(String user) {
        if (!userToResidenceMap.containsKey(user)) {
            throw new RuntimeException("user " + user + " doesn't exist");
        }

        return userToResidenceMap.get(user);
    }

    private Map<String, String> loadUsersResidenceFromFile() {
        Map<String, String> userToResidence = new HashMap<>();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(userResidenceFile);

        Scanner scanner = new Scanner(inputStream);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String []userResidencePair = line.split(" ");
            userToResidence.put(userResidencePair[0], userResidencePair[1]);
        }
        return Collections.unmodifiableMap(userToResidence);
    }
}
