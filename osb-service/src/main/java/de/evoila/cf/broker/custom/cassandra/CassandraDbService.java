package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author Johannes Hiemer.
 */
public class CassandraDbService {

    private Logger log = LoggerFactory.getLogger(getClass());

    private CqlSession session;

    public boolean createConnection(String username, String password, String database, List<ServerAddress> serverAddresses) {
        try {
            String keyspace = database;

            CqlSessionBuilder sessionBuilder = CqlSession.builder();
            for (ServerAddress sA : serverAddresses)
                sessionBuilder.addContactPoint(new InetSocketAddress(sA.getIp(), sA.getPort()));

            session = sessionBuilder.withKeyspace(keyspace).build();
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
