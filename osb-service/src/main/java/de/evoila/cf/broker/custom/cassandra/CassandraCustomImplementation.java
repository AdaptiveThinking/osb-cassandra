package de.evoila.cf.broker.custom.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import de.evoila.cf.broker.bean.ExistingEndpointBean;
import de.evoila.cf.broker.model.Platform;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.UsernamePasswordCredential;
import de.evoila.cf.broker.util.ServiceInstanceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Johannes Hiemer.
 */
@Service
public class CassandraCustomImplementation {

    private Logger log = LoggerFactory.getLogger(CassandraCustomImplementation.class);

    private CassandraExistingEndpointBean cassandraExistingEndpointBean;

    public CassandraCustomImplementation(CassandraExistingEndpointBean cassandraExistingEndpointBean) {
        this.cassandraExistingEndpointBean = cassandraExistingEndpointBean;
    }

    public void createDatabase(CassandraDbService cassandraDbService, String database) {
        String createStatement = "CREATE KEYSPACE " + database + " WITH " +
                "replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
        cassandraDbService.executeStatement(createStatement, ConsistencyLevel.LOCAL_QUORUM);
        this.createRoleAndPermissions(cassandraDbService, database);
        // sleep to allow for the propagation of new settings throughout the cluster
//        try { Thread.sleep(5000); } catch(InterruptedException e){}

    }

    private void createRoleAndPermissions(CassandraDbService cassandraDbService, String database) {

        String roleAdminStatement = "CREATE ROLE IF NOT EXISTS " + database + "_admin;";
        cassandraDbService.executeStatement(roleAdminStatement, ConsistencyLevel.LOCAL_QUORUM);

        String permissionAdminStatement = "GRANT ALL PERMISSIONS on KEYSPACE " + database + " TO " + database + "_admin;";
        cassandraDbService.executeStatement(permissionAdminStatement, ConsistencyLevel.LOCAL_QUORUM);

        String roleStatement = "CREATE ROLE IF NOT EXISTS " + database + "_user;";
        cassandraDbService.executeStatement(roleStatement, ConsistencyLevel.LOCAL_QUORUM);

        String permissionStatement = "GRANT ALL PERMISSIONS on KEYSPACE " + database + " TO " + database + "_user;";
        cassandraDbService.executeStatement(permissionStatement, ConsistencyLevel.LOCAL_QUORUM);
    }

    public void deleteDatabase(CassandraDbService cassandraDbService, String database) {
        String cqlStatement = "DROP KEYSPACE IF EXISTS " + database + ";";

        cassandraDbService.executeStatement(cqlStatement, ConsistencyLevel.ONE);

        this.dropRoles(cassandraDbService, database);
    }

    public void dropRoles(CassandraDbService cassandraDbService, String database) {
        String dropRoleAdminStatement = "DROP ROLE IF EXISTS \'" + database + "_admin\';";
        cassandraDbService.executeStatement(dropRoleAdminStatement, ConsistencyLevel.ONE);

        String dropRoleUserStatement = "DROP ROLE IF EXISTS \'" + database + "_user\';";
        cassandraDbService.executeStatement(dropRoleUserStatement, ConsistencyLevel.ONE);
    }

    public void bindRoleToDatabase(CassandraDbService cassandraDbService, String username,
                                   String password, String database) {
        String cqlStatement = "CREATE ROLE " + username + " WITH LOGIN = true AND PASSWORD = '" + password + "';";
        cassandraDbService.executeStatement(cqlStatement, ConsistencyLevel.LOCAL_QUORUM);

        String roleBinding = "GRANT " + database + "_user TO " + username + ";";
        cassandraDbService.executeStatement(roleBinding, ConsistencyLevel.LOCAL_QUORUM);
    }

    public void unbindRoleFromDatabase(CassandraDbService cassandraDbService, String username) {
        String cqlStatement = "DROP ROLE IF EXISTS " + username + ";";
        cassandraDbService.executeStatement(cqlStatement, ConsistencyLevel.ONE);
    }

    public CassandraDbService connection(ServiceInstance serviceInstance, Plan plan, UsernamePasswordCredential usernamePasswordCredential) {
        CassandraDbService cassandraService = new CassandraDbService();

        if (plan.getPlatform() == Platform.BOSH) {
            List<ServerAddress> serverAddresses = serviceInstance.getHosts();

            if (plan.getMetadata().getIngressInstanceGroup() != null &&
                    plan.getMetadata().getIngressInstanceGroup().length() > 0)
                serverAddresses = ServiceInstanceUtils.filteredServerAddress(serviceInstance.getHosts(),
                        plan.getMetadata().getIngressInstanceGroup());

            cassandraService.createConnection(usernamePasswordCredential.getUsername(), usernamePasswordCredential.getPassword(),
                   CassandraUtils.dbName(serviceInstance.getId()), null, serverAddresses);
        } else if (plan.getPlatform() == Platform.EXISTING_SERVICE)
            cassandraService.createConnection(cassandraExistingEndpointBean.getUsername(), cassandraExistingEndpointBean.getPassword(),
                    cassandraExistingEndpointBean.getDatabase(), cassandraExistingEndpointBean.getDatacenter(), cassandraExistingEndpointBean.getHosts());

        return cassandraService;
    }

    public void closeConnection(CassandraDbService cassandraDbService) {
        cassandraDbService.closeConnection();
    }
}
