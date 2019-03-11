package de.evoila.cf.broker.custom.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Johannes Hiemer.
 */
public class CassandraDbService {

    private Logger log = LoggerFactory.getLogger(getClass());

    private Session session;

    public boolean createConnection(String username, String password, String database, List<ServerAddress> serverAddresses) {
        try {
            String keyspace = database;
            Cluster cluster = Cluster.builder()
                    .addContactPoints(serverAddresses.stream().map(s -> s.getIp()).toArray(String[]::new))
                    .withCredentials(username, password)
                    .build();
            session = cluster.connect(keyspace);
        } catch (Exception e) {
            log.info("Could not establish client", e);
            return false;
        }
        return true;
    }

    public void closeConnection() {
        session.close();
    }

    public void executeStatement(String statement) {
        session.execute(statement);
    }
}
