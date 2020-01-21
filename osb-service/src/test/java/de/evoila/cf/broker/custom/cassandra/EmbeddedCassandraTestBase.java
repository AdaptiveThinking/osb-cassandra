package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is the base for the other Unit tests.
 * It starts the embedded cassandra instance before all tests cases.
 * It provides a creation of a connection to cassandra before a test case
 * and cleanup of created keyspaces and the session after a test case.
 * Additionally reoccuring variables are stored as constants, such as {@linkplain #USERNAME}, {@linkplain #DATACENTER} and more.
 */
public class EmbeddedCassandraTestBase {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedCassandraTestBase.class);

    public static final String USERNAME = "cassandra";
    public static final String PASSWORD = "cassandra";
    public static final String DATACENTER = "datacenter1";
    public static final String DEFAULT_KEYSPACE = "system";

    public static final String KEYSPACE_NAME = "test_keyspace";
    public static final String TEST_USER_NAME = "test_user";
    public static final String TEST_USER_PASSWORD= "test_password";


    private static int port;
    private static String ip;
    private static String clusterName;

    static CassandraDbService cassandraDbService;
    static CassandraCustomImplementation cassandraImplementation;

    @Before
    public void prepareConnection() {
        prepareConnection(DEFAULT_KEYSPACE, USERNAME, PASSWORD);
    }

    public void prepareConnection(String database, String username, String password) {
        List<ServerAddress> addresses = new LinkedList<>();
        addresses.add(new ServerAddress("embedded_cassandra",ip, port));

        cassandraDbService.createConnection(username,
                    password,
                    database,
                    DATACENTER,
                    addresses);
    }

    @BeforeClass
    public static void prepareEmbeddedCassandra() throws IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("embedded_cassandra.yml");
        ip = EmbeddedCassandraServerHelper.getHost();
        port = EmbeddedCassandraServerHelper.getNativeTransportPort();
        clusterName = EmbeddedCassandraServerHelper.getClusterName();

        // Wait for cluster to be started completely, including the superuser to be created
        try { Thread.sleep(20000); } catch(InterruptedException e){}
        log.info("Created embedded cassandra under \"" + clusterName + "\" - " + ip + ":"+port);

        cassandraDbService = new CassandraDbService();
        cassandraImplementation = new CassandraCustomImplementation(null);
    }

    @After
    public void cleanUpCassandra() {
        dropNonSystemKeyspaces();
        cassandraDbService.closeConnection();
    }

    /**
     * Drops all keyspaces from Cassandra that are not system keyspaces, aka its does not start with 'system_'
     * Need a custom implementation instead of using {@linkplain EmbeddedCassandraServerHelper#cleanEmbeddedCassandra()} because the session of the Helper class is always null.
     * This problem seems to be caused by the initialization of session at a time, the default superuser does not exist yet.
     */
    private void dropNonSystemKeyspaces() {

        ResultSet resultSet = cassandraDbService.executeStatement("SELECT keyspace_name FROM system_schema.keyspaces;", ConsistencyLevel.ONE);
        Iterator<Row> iterator = resultSet.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String keyspaceName = row.getString("keyspace_name");
            if (!keyspaceName.startsWith("system")) {
                cassandraDbService.executeStatement("DROP KEYSPACE IF EXISTS " + keyspaceName, ConsistencyLevel.ONE);
            }
        }
    }
}
