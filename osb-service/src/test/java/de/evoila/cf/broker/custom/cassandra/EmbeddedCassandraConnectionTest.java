package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.junit.*;

import java.util.List;

import static org.springframework.test.util.AssertionErrors.assertTrue;

public class EmbeddedCassandraConnectionTest extends EmbeddedCassandraTestBase {

    /**
     * Simply creates a connection and looks for the existence of the system keyspace.
     * This tests the {@linkplain CassandraDbService#createConnection(String, String, String, String, List)} method.
     */
    @Test
    public void connectionTest() {
        prepareConnection();
        ResultSet resultSet = cassandraDbService.executeStatement("SELECT keyspace_name from system_schema.keyspaces;");
        assertTrue("Querying for keyspaces returned an empty set, although system keyspaces should be present.", resultSet.iterator().hasNext());
    }
}
