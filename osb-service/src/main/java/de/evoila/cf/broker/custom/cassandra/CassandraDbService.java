package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;

/**
 * @author Johannes Hiemer.
 */
public class CassandraDbService {

    /**
     * The default value for the datacenter field for connections with cassandra.
     * This default is derived from the static name, that is used in all bosh deployments that this
     * broker uses at the time of the addition of this constant.
     */
    public static final String DATACENTER_DEFAULT = "bosh-dc1";

    /**
     * Cassandra might still be in the process of creation the user, which is used to authenticate.
     * To encounter this time based error, a retry mechanism is build in, which does a maximum of retries
     * configured in this constant.
     */
    public static final int MAX_CONNECTION_TRIES = 10;
    public static final int CONNECTION_RETRY_DELAY = 5 * 1000;

    private Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Session for communicating with cassandra.
     * To open a connection call {@linkplain #createConnection(String, String, String, String, List)}.
     * To check for connection status call {@linkplain #isConnected()}.
     * To close the connection call {@linkplain #closeConnection()}
     */
    private CqlSession session;

    /**
     * Config singleton object for the cassandra driver.
     * Get access to this singleton via {@linkplain #getCassandraConfigHolder()}.
     */
    private static DriverConfigLoader cassandraConfigHolder;

    /**
     * Creates a connection with cassandra by using the input parameters.
     * The created session is stored in the object for later usage via {@linkplain #executeStatement(String)}.
     * It is recommended to close this connection after usage instead of keeping it open the whole time.
     * The connection implements an AutoClose feature, but it is recommended to {@linkplain #closeConnection()} in a controlled way.
     *
     * Due to the nature of cassandra, a retry mechanism is added to counter the perk of cassandra,
     * that a role / user creation operation can be running async and makes room for connection tries with
     * a not yet created role / user, which will end in a bad credentials error. The connection is retried until
     * {@linkplain #MAX_CONNECTION_TRIES} is reached or the connection is established.
     *
     * @param username to authenticate against cassandra
     * @param password to authenticate against cassandra
     * @param database holds the name of the keyspace in cassandra to target
     * @param datacenter to target at the cassandra instance. Defaults to {@linkplain #DATACENTER_DEFAULT} if value is null or empty.
     * @param serverAddresses list of cassandra ServerAddresses to connect with.
     * @return a flag that indicates the connection status
     */
    public boolean createConnection(String username, String password, String database, String datacenter, List<ServerAddress> serverAddresses) {
        String keyspace = database;
        if (StringUtils.isEmpty(datacenter)) {
            datacenter = DATACENTER_DEFAULT;
        }

        log.info("Gather contact points:");
        CqlSessionBuilder sessionBuilder = CqlSession.builder();
        for (ServerAddress sA : serverAddresses) {
            log.info(" ... server: " + sA.getIp() + "/" + sA.getPort());
            sessionBuilder.addContactPoint(new InetSocketAddress(sA.getIp(), sA.getPort()))
                    .withAuthCredentials(username,password);
        }

        sessionBuilder.withKeyspace(keyspace)
                .withLocalDatacenter(datacenter)
                .withConfigLoader(getCassandraConfigHolder());

        log.info("Set credentials for user: " + username);

        int tries = 0;
        while (!isConnected() && tries < MAX_CONNECTION_TRIES) {
            try {
                session = sessionBuilder.build();
            } catch (Exception e) {
                tries++;
                log.info("Could not establish client (" + tries + " of " + MAX_CONNECTION_TRIES + " tries)", e);
                try {
                    log.info("Waiting "+CONNECTION_RETRY_DELAY+"ms until the next try ...");
                    Thread.sleep(CONNECTION_RETRY_DELAY);
                } catch (InterruptedException ex) {
                    log.debug("Retry sleep was interrupted.", ex);
                }
            }
        }

        return isConnected();
    }

    /**
     * Checks and returns the connection state of the underlying {@linkplain #session}.
     * @return false if session is null or {@linkplain CqlSession#isClosed()}.
     */
    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    /**
     * Closes the session, if it is still open.
     */
    public void closeConnection() {
        if (isConnected())
            session.close();
    }

    /**
     * Executes a Cassandra statement via the underlying {@linkplain #session}.
     * This methods performs no checks, whether the session exists or is already closed!
     * @param query string of a cassandra statement to execute
     * @return the {@linkplain ResultSet} object
     */
    public ResultSet executeStatement(String query, ConsistencyLevel consistency) {
        SimpleStatement statement = SimpleStatement.newInstance(query).setConsistencyLevel(consistency);
        return session.execute(statement);
    }

    /**
     * Returns the CassandraConfigHolder singleton that is embedded in this class.
     * It is used to set username and password for authentication against cassandra.
     * Furthermore sets the timeout of requests to 60 seconds to prevent errors by default timeout.
     * Following four options are set:
     * - DefaultDriverOption.AUTH_PROVIDER_CLASS
     * - DefaultDriverOption.AUTH_PROVIDER_USER_NAME
     * - DefaultDriverOption.AUTH_PROVIDER_PASSWORD
     * - DefaultDriverOption.REQUEST_TIMEOUT
     * @return a DriverConfigLoader with the four above listed options.
     */
    public DriverConfigLoader getCassandraConfigHolder() {
        if (cassandraConfigHolder == null) {
            cassandraConfigHolder = DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(60))
                    .build();
        }
        return cassandraConfigHolder;
    }
}
