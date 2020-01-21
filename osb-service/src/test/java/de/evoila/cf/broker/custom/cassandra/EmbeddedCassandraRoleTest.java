package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.StringUtils;

import java.util.Optional;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmbeddedCassandraRoleTest extends EmbeddedCassandraTestBase {


    /**
     * Creates a keyspace to use in the {@linkplain #roleTest()}.
     */
    @Before
    public void prepareKeyspace() {
        cassandraImplementation.createDatabase(cassandraDbService, KEYSPACE_NAME);
    }

    /**
     * Binds a role to a keyspace via the {@linkplain CassandraCustomImplementation} and checks whether the expected entries are made in cassandra.
     */
    @Test
    public void roleTest() {
        cassandraImplementation.bindRoleToDatabase(cassandraDbService, TEST_USER_NAME, TEST_USER_PASSWORD, KEYSPACE_NAME);

        ResultSet resultSet = cassandraDbService.executeStatement("SELECT * FROM system_auth.roles;", ConsistencyLevel.LOCAL_QUORUM);
        Optional<Row> row = StreamSupport.stream(resultSet.spliterator(), false)
                .filter(r -> r.getString("role").equals(TEST_USER_NAME))
                .findAny();
        assertTrue("Querying for a new role did not create a new role.", row.isPresent());
        assertTrue("Created user can not login, but should be able to.", row.get().getBoolean("can_login"));
        assertFalse("Created user is superuser, but should not be.", row.get().getBoolean("is_superuser"));
        if (!StringUtils.isEmpty(TEST_USER_PASSWORD))
            assertFalse("Created user should have an hashed password, but does not.", StringUtils.isEmpty(row.get().getString("salted_hash")));
        assertTrue("", row.get().getSet("member_of", String.class).contains(KEYSPACE_NAME + "_user"));

        cassandraImplementation.unbindRoleFromDatabase(cassandraDbService, TEST_USER_NAME);
        resultSet = cassandraDbService.executeStatement("SELECT * FROM system_auth.roles;", ConsistencyLevel.LOCAL_QUORUM);
        row = StreamSupport.stream(resultSet.spliterator(), false)
                .filter(r -> r.getString("role").equals(TEST_USER_NAME))
                .findAny();
        assertTrue("Role should be deleted, but is not.", row.isEmpty());
    }
}
