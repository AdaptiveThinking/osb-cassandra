package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
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

    private Logger log = LoggerFactory.getLogger(getClass());

    private CqlSession session;

    private static DriverConfigLoader cassandraConfigHolder;

    /**
     * Creates a connection with cassandra by using the input parameters.
     * The created session is stored in the object for later usage via {@linkplain #executeStatement(String)}.
     * It is recommended to close this connection after usage instead of keeping it open the whole time.
     * The connection implements an AutoClose feature, but it is recommended to {@linkplain #closeConnection()} in a controlled way.
     * @param username to authenticate against cassandra
     * @param password to authenticate against cassandra
     * @param database holds the name of the keyspace in cassandra to target
     * @param datacenter to target at the cassandra instance. Defaults to {@linkplain #DATACENTER_DEFAULT} if value is null or empty.
     * @param serverAddresses list of cassandra ServerAddresses to connect with.
     * @return a flag that indicates the connection status
     */
    public boolean createConnection(String username, String password, String database, String datacenter, List<ServerAddress> serverAddresses) {
        try {
            String keyspace = database;
            if (StringUtils.isEmpty(datacenter))
                datacenter = DATACENTER_DEFAULT;

            CqlSessionBuilder sessionBuilder = CqlSession.builder();
            for (ServerAddress sA : serverAddresses)
                sessionBuilder.addContactPoint(new InetSocketAddress(sA.getIp(), sA.getPort()));
            session = sessionBuilder
                    .withKeyspace(keyspace)
                    .withLocalDatacenter(datacenter)
                    .withConfigLoader(getCassandraConfigHolder(username, password))
                    .build();
        } catch (Exception e) {
            log.info("Could not establish client", e);
            return false;
        }
        return true;
    }

    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    public void closeConnection() {
        session.close();
    }

    public ResultSet executeStatement(String statement) {
        return session.execute(statement);
    }

    /**
     * Returns the CassandraConfigHolder singleton that is embedded in this class.
     * It is used to set username and password for authentication against cassandra.
     * Following three options are set:
     * - DefaultDriverOption.AUTH_PROVIDER_CLASS
     * - DefaultDriverOption.AUTH_PROVIDER_USER_NAME
     * - DefaultDriverOption.AUTH_PROVIDER_PASSWORD
     * @param username to authenticate against cassandra
     * @param password to authenticate against cassandra
     * @return a DriverConfigLoader with the three above listed options.
     */
    public DriverConfigLoader getCassandraConfigHolder(String username, String password) {
        if (cassandraConfigHolder == null) {
            cassandraConfigHolder = DriverConfigLoader.programmaticBuilder()
                    .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, username)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, password)
                    .build();
        }
        return cassandraConfigHolder;
    }
}
