package de.evoila.cf.broker.custom.cassandra;

import de.evoila.cf.broker.exception.ServiceBrokerException;
import de.evoila.cf.broker.model.RouteBinding;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.ServiceInstanceBinding;
import de.evoila.cf.broker.model.ServiceInstanceBindingRequest;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.UsernamePasswordCredential;
import de.evoila.cf.broker.repository.*;
import de.evoila.cf.broker.service.AsyncBindingService;
import de.evoila.cf.broker.service.impl.BindingServiceImpl;
import de.evoila.cf.broker.util.ServiceInstanceUtils;
import de.evoila.cf.cpi.bosh.CredentialConstants;
import de.evoila.cf.cpi.existing.CassandraExistingServiceFactory;
import de.evoila.cf.security.credentials.CredentialStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Johannes Hiemer.
 */
@Service
public class CassandraBindingService extends BindingServiceImpl {

	private Logger log = LoggerFactory.getLogger(getClass());

    private static String URI = "uri";

    private static String USERNAME = "user";

    private static String PASSWORD = "password";

    private static String DATABASE = "database";

    private CredentialStore credentialStore;

    private CassandraExistingServiceFactory cassandraExistingServiceFactory;

    private CassandraCustomImplementation cassandraCustomImplementation;

    public CassandraBindingService(BindingRepository bindingRepository, ServiceDefinitionRepository serviceDefinitionRepository,
                                   CassandraExistingServiceFactory cassandraExistingServiceFactory,
                                   CassandraCustomImplementation cassandraCustomImplementation,
                                   ServiceInstanceRepository serviceInstanceRepository, RouteBindingRepository routeBindingRepository,
                                   CredentialStore credentialStore,
                                   JobRepository jobRepository, AsyncBindingService asyncBindingService,
                                   PlatformRepository platformRepository) {
        super(bindingRepository, serviceDefinitionRepository,
                serviceInstanceRepository, routeBindingRepository,
                jobRepository,
                asyncBindingService, platformRepository);
        this.cassandraExistingServiceFactory = cassandraExistingServiceFactory;
        this.cassandraCustomImplementation = cassandraCustomImplementation;
        this.credentialStore = credentialStore;
    }

    @Override
	protected Map<String, Object> createCredentials(String bindingId, ServiceInstanceBindingRequest serviceInstanceBindingRequest,
                                                    ServiceInstance serviceInstance, Plan plan, ServerAddress host) throws ServiceBrokerException {
        UsernamePasswordCredential serviceInstanceUsernamePasswordCredential = credentialStore
                .getUser(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS);

        String database = CassandraUtils.dbName(serviceInstance.getId());
        if (serviceInstanceBindingRequest.getParameters() != null) {
            String customBindingDatabase = (String) serviceInstanceBindingRequest.getParameters().get(DATABASE);

            if (!StringUtils.isEmpty(customBindingDatabase))
                database = customBindingDatabase;
        }

        credentialStore.createUser(serviceInstance, bindingId);
        UsernamePasswordCredential usernamePasswordCredential = credentialStore.getUser(serviceInstance, bindingId);

        CassandraDbService cassandraDbService = cassandraCustomImplementation.connection(serviceInstance, plan, serviceInstanceUsernamePasswordCredential);
        cassandraCustomImplementation.bindRoleToDatabase(cassandraDbService, usernamePasswordCredential.getUsername(),
                usernamePasswordCredential.getPassword(), database);
        cassandraCustomImplementation.closeConnection(cassandraDbService);

        List<ServerAddress> cassandraHosts = serviceInstance.getHosts();
        String ingressInstanceGroup = plan.getMetadata().getIngressInstanceGroup();
        if (ingressInstanceGroup != null && ingressInstanceGroup.length() > 0) {
            cassandraHosts = ServiceInstanceUtils.filteredServerAddress(serviceInstance.getHosts(), ingressInstanceGroup);
        }

        String endpoint = ServiceInstanceUtils.connectionUrl(cassandraHosts);

        // When host is not empty, it is a service key
        if (host != null)
            endpoint = host.getIp() + ":" + host.getPort();

        String dbURL = String.format("cassandra://%s:%s@%s/%s", usernamePasswordCredential.getUsername(),
                usernamePasswordCredential.getPassword(), endpoint, database);

        Map<String, Object> credentials = new HashMap<>();
        credentials.put(URI, dbURL);
        credentials.put(USERNAME, usernamePasswordCredential.getUsername());
        credentials.put(PASSWORD, usernamePasswordCredential.getPassword());
        credentials.put(DATABASE, database);

        return credentials;
	}

    @Override
    protected void unbindService(ServiceInstanceBinding binding, ServiceInstance serviceInstance, Plan plan)  {
        UsernamePasswordCredential serviceInstanceUsernamePasswordCredential = credentialStore
                .getUser(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS);

        UsernamePasswordCredential usernamePasswordCredential = credentialStore.getUser(serviceInstance, binding.getId());

        CassandraDbService cassandraDbService = cassandraCustomImplementation.connection(serviceInstance, plan, serviceInstanceUsernamePasswordCredential);
        cassandraCustomImplementation.unbindRoleFromDatabase(cassandraDbService, usernamePasswordCredential.getUsername());
        cassandraCustomImplementation.closeConnection(cassandraDbService);

        credentialStore.deleteCredentials(serviceInstance, binding.getId());
    }

    @Override
    protected RouteBinding bindRoute(ServiceInstance serviceInstance, String route) {
        throw new UnsupportedOperationException();
    }

}
