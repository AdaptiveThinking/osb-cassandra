package de.evoila.cf.broker.custom.cassandra;

/**
 * @author Johannes Hiemer.
 */
public class CassandraUtils {

    public static String dbName(String uuid) {
        if (uuid != null && uuid.length() > 15)
            return "d" + uuid.replace("-", "")
                    .substring(0, 15)
                    .toLowerCase();
        else
            return uuid;
    }

}
