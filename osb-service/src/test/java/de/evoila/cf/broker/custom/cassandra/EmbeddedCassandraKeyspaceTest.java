package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EmbeddedCassandraKeyspaceTest extends EmbeddedCassandraTestBase {

    public static final String[] ADMIN_PERMISSIONS = new String[] {"ALTER", "AUTHORIZE", "CREATE", "DROP", "MODIFY", "SELECT"};
    public static final String[] USER_PERMISSIONS = new String[] {"ALTER", "AUTHORIZE", "CREATE", "DROP", "MODIFY", "SELECT"};


    @Test
    public void keyspaceTest() {
        // Test keyspace creation
        cassandraImplementation.createDatabase(cassandraDbService, "test_keyspace");
        ResultSet resultSet = cassandraDbService.executeStatement("SELECT * from system_schema.keyspaces;");

        Optional<Row> row = StreamSupport.stream(resultSet.spliterator(), false)
                .filter(r -> r.getString("keyspace_name").equals("test_keyspace"))
                .findAny();
        assertTrue("Querying for a new keyspace did not create the keyspace.", row.isPresent());


        // Test admin creation and permissions
        String keyspaceAdmin= KEYSPACE_NAME+"_admin";
        checkExistenceOfUser(keyspaceAdmin);
        checkExistingPermissionsOfUser(keyspaceAdmin, Arrays.asList(ADMIN_PERMISSIONS));

        // Test user creation and permission
        String keyspaceUser= KEYSPACE_NAME+"_user";
        checkExistenceOfUser(keyspaceUser);
        checkExistingPermissionsOfUser(keyspaceUser, Arrays.asList(USER_PERMISSIONS));


        // Test keyspace deletion
        cassandraImplementation.deleteDatabase(cassandraDbService, KEYSPACE_NAME);
        resultSet = cassandraDbService.executeStatement("SELECT * from system_schema.keyspaces;");

        row = StreamSupport.stream(resultSet.spliterator(), false)
                .filter(r -> r.getString("keyspace_name").equals("test_keyspace"))
                .findAny();
        assertTrue("Querying for a deletion of a keyspace did not delete the keyspace.", row.isEmpty());

        // Test admin deletion and permissions
        checkAbsenceOfUser(keyspaceAdmin);
        checkAbsencePermissionsOfUser(keyspaceAdmin);

        // Test user deletion and permissions
        checkAbsenceOfUser(keyspaceUser);
        checkAbsencePermissionsOfUser(keyspaceUser);
    }

    private void checkExistenceOfUser(String username) {
        ResultSet resultSet = getUser(username);
        assertTrue("An user \'"+username+"\' for the keyspace should have been created, but was not.", resultSet.iterator().hasNext());
    }

    private void checkAbsenceOfUser(String username) {
        ResultSet resultSet = getUser(username);
        assertFalse("An user \'"+username+"\' for the keyspace should have been deleted, but was not.", resultSet.iterator().hasNext());
    }

    private ResultSet getUser(String username) {
        return cassandraDbService.executeStatement("SELECT * FROM system_auth.roles WHERE role = \'"+username+"\';");
    }

    private void checkExistingPermissionsOfUser(String username, List<String> permissions) {
        ResultSet resultSet = getPermissionsOfUser(username);
        Optional<Row> row = StreamSupport.stream(resultSet.spliterator(), false)
                .filter(r -> r.getString("role").equals(username))
                .findAny();
        assertTrue("User \'"+username+"\' should have a permissions row, but has none.", row.isPresent());

        Set<String> permissionsFound = row.get().getSet("permissions", String.class);
        assertTrue("User \'"+username+"\' holds different amount of permissions than expected. Expected " + ADMIN_PERMISSIONS.length + " but found " + permissions.size()
                , permissionsFound.size() == permissions.size());
        assertTrue("User \'"+username+"\' does not hold all expected permissions after granting permissions. Expected " + permissions.toString() + " but found " + permissionsFound.toString()
                , permissionsFound.containsAll(permissions));
    }

    private void checkAbsencePermissionsOfUser(String username) {
        ResultSet resultSet = getPermissionsOfUser(username);
        assertFalse("User \'"+username+"\' should have no permissions, but a permissions entry was found.", resultSet.iterator().hasNext());
    }

    private ResultSet getPermissionsOfUser(String username) {
        return cassandraDbService.executeStatement("SELECT * FROM system_auth.role_permissions WHERE role = \'" + username + "\';");
    }
}
