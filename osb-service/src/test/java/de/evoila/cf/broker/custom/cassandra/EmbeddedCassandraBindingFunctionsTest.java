package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmbeddedCassandraBindingFunctionsTest extends EmbeddedCassandraTestBase {

    public static final String TEST_TABLE= "test_table";

    private static final String SELECT_TEST_KEYSPACE = "SELECT * from system_schema.tables WHERE keyspace_name = \'"+KEYSPACE_NAME+"\';";
    private static final String SELECT_FROM_TABLE_QUERY= "SELECT * from "+KEYSPACE_NAME+"."+TEST_TABLE+";";

    /**
     * Mirrors a binding and switches to the user created in the binding.
     */
    @Before
    public void prepareUserAndConnection() {
        cassandraImplementation.createDatabase(cassandraDbService, KEYSPACE_NAME);
        cassandraImplementation.bindRoleToDatabase(cassandraDbService, TEST_USER_NAME, TEST_USER_PASSWORD, KEYSPACE_NAME);
        cassandraDbService.closeConnection();
        prepareConnection(KEYSPACE_NAME, TEST_USER_NAME, TEST_USER_PASSWORD);
    }

    /**
     * This tests runs commands on a binding to verify the integrity of a binding.
     * Therefore following steps are done as the binding user:
     *  <ul>
     *      <li>Create a table</li>
     *      <li>Insert data</li>
     *      <li>Delete data</li>
     *      <li>Trunk table</li>
     *      <li>Drop table</li>
     *  </ul>
     *
     * For preparations see {@linkplain #prepareUserAndConnection()}.
     */
    @Test
    public void basicBindingFunctionsTest() {
        // Test table non-existence
        ResultSet resultSet = cassandraDbService.executeStatement(SELECT_TEST_KEYSPACE, ConsistencyLevel.ONE);
        assertFalse("Test table should not exist yet.", resultSet.iterator().hasNext());

        // Test CREATE TABLE permission
        cassandraDbService.executeStatement("CREATE TABLE "+KEYSPACE_NAME+"."+TEST_TABLE+"(" +
                "id int PRIMARY KEY," +
                "test_column1 text," +
                "test_column2 int" +
                ");", ConsistencyLevel.LOCAL_QUORUM);
        resultSet = cassandraDbService.executeStatement("SELECT * from system_schema.keyspaces WHERE keyspace_name = \'"+KEYSPACE_NAME+"\';", ConsistencyLevel.LOCAL_QUORUM);
        assertTrue("Querying for a new table did not create a table.", resultSet.iterator().hasNext());

        resultSet = cassandraDbService.executeStatement(SELECT_FROM_TABLE_QUERY, ConsistencyLevel.LOCAL_QUORUM);
        assertFalse("Table should be empty, but is not.", resultSet.iterator().hasNext());


        // Test INSERT permission
        cassandraDbService.executeStatement("INSERT INTO "+TEST_TABLE+" (id, test_column1, test_column2) VALUES(" +
                "1, \'test\', 42);", ConsistencyLevel.LOCAL_QUORUM);
        cassandraDbService.executeStatement("INSERT INTO "+TEST_TABLE+" (id, test_column1, test_column2) VALUES(" +
                "2, \'testing\', 99);", ConsistencyLevel.LOCAL_QUORUM);
        resultSet = cassandraDbService.executeStatement(SELECT_FROM_TABLE_QUERY, ConsistencyLevel.LOCAL_QUORUM);
        assertTrue("There should be at least one row in the table.", resultSet.iterator().hasNext() && resultSet.iterator().next() != null);
        assertTrue("There should be two rows in the table, but are not.", resultSet.iterator().hasNext() && resultSet.iterator().next() != null);
        assertFalse("There should be not more than two rows, but found a third one.", resultSet.iterator().hasNext());

        // Test DELETE permission
        cassandraDbService.executeStatement("DELETE FROM "+KEYSPACE_NAME+"."+TEST_TABLE+" WHERE id = 1;", ConsistencyLevel.LOCAL_QUORUM);
        resultSet = cassandraDbService.executeStatement(SELECT_FROM_TABLE_QUERY, ConsistencyLevel.LOCAL_QUORUM);
        assertTrue("There should be one row in the table after deleting the second one.", resultSet.iterator().hasNext() && resultSet.iterator().next() != null);
        assertFalse("There should be not more than one row after deleting one, but found a second one.", resultSet.iterator().hasNext());

        // Test TRUNK permission
        cassandraDbService.executeStatement("TRUNCATE TABLE "+KEYSPACE_NAME+"."+TEST_TABLE+";", ConsistencyLevel.LOCAL_QUORUM);
        resultSet = cassandraDbService.executeStatement(SELECT_FROM_TABLE_QUERY, ConsistencyLevel.LOCAL_QUORUM);
        assertFalse("Table should be empty after truncate, but found at least one row.", resultSet.iterator().hasNext());

        // Test DROP TABLE permission
        cassandraDbService.executeStatement("DROP TABLE "+KEYSPACE_NAME+"."+TEST_TABLE+";", ConsistencyLevel.LOCAL_QUORUM);
        resultSet = cassandraDbService.executeStatement(SELECT_TEST_KEYSPACE, ConsistencyLevel.LOCAL_QUORUM);
        assertFalse("Table should not exist after dropping it, but it does.", resultSet.iterator().hasNext());
    }

    @After
    public void removeRoleAndKeyspace() {
        cassandraImplementation.unbindRoleFromDatabase(cassandraDbService, TEST_USER_NAME);
        cassandraImplementation.deleteDatabase(cassandraDbService, KEYSPACE_NAME);
    }
}
