package de.evoila.cf.broker.custom.cassandra;

import de.evoila.cf.broker.util.UuidUtils;

/**
 * @author Johannes Hiemer, Johannes Strauß.
 */
public class CassandraUtils {

    public static String dbName(String uuid) {
        if (!UuidUtils.isValidUUID(uuid)) {
            throw new IllegalArgumentException("Please use valid UUIDs to create Cassandra database names!");
        }

        return "d" + uuid.replace("-", "")
                .substring(0, 15)
                .toLowerCase();
    }
}