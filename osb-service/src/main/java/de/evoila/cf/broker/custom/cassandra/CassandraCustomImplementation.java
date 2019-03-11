/**
 *
 */
package de.evoila.cf.broker.custom.cassandra;

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

    private ExistingEndpointBean existingEndpointBean;

    public CassandraCustomImplementation(ExistingEndpointBean existingEndpointBean) {
        this.existingEndpointBean = existingEndpointBean;
    }

    public void createDatabase(CassandraDbService cassandraDbService, String database) {
        String createStatement = "CREATE KEYSPACE " + database + " WITH " +
                "replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        cassandraDbService.executeStatement(createStatement);
    }

    private void createRoleAndPermissions(CassandraDbService cassandraDbService, String database) {
        String roleAdminStatement = "CREATE ROLE IF NOT EXISTS " + database + "_admin;";
        cassandraDbService.executeStatement(roleAdminStatement);

        String permissionAdminStatement = "GRANT ALL PERMISSIONS on KEYSPACE " + database + " TO " + database + "_admin;";
        cassandraDbService.executeStatement(permissionAdminStatement);

        String roleStatement = "CREATE ROLE IF NOT EXISTS " + database + "_user;";
        cassandraDbService.executeStatement(roleStatement);

        String permissionStatement = "GRANT ALL PERMISSIONS on KEYSPACE " + database + " TO " + database + "_user;";
        cassandraDbService.executeStatement(permissionStatement);
    }

    public void deleteDatabase(CassandraDbService cassandraDbService, String database) {
        String cqlStatement = "CREATE KEYSPACE " + database + " WITH " +
                "replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";

        cassandraDbService.executeStatement(cqlStatement);
    }

    public void bindRoleToDatabase(CassandraDbService cassandraDbService, String username,
                                   String password, String database) {
        String cqlStatement = "CREATE ROLE " + username + " WITH LOGIN = true AND PASSWORD = '" + password + "';";
        cassandraDbService.executeStatement(cqlStatement);

        String roleBinding = "GRANT " + database + "_user TO " + username + ";";
        cassandraDbService.executeStatement(roleBinding);
    }

    public void unbindRoleFromDatabase(CassandraDbService cassandraDbService, String username) {
        String cqlStatement = "DROP ROLE IF EXISTS " + username + ";";
        cassandraDbService.executeStatement(cqlStatement);
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
                   CassandraUtils.dbName(serviceInstance.getId()), serverAddresses);
        } else if (plan.getPlatform() == Platform.EXISTING_SERVICE)
            cassandraService.createConnection(existingEndpointBean.getUsername(), existingEndpointBean.getPassword(),
                    existingEndpointBean.getDatabase(), existingEndpointBean.getHosts());

        return cassandraService;
    }

    public void closeConnection(CassandraDbService cassandraDbService) {
        cassandraDbService.closeConnection();
    }
}
