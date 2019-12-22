/**
 * 
 */
package de.evoila.cf.cpi.existing;


import de.evoila.cf.broker.bean.impl.ExistingEndpoint;
import de.evoila.cf.broker.custom.cassandra.CassandraCustomImplementation;
import de.evoila.cf.broker.custom.cassandra.CassandraDbService;
import de.evoila.cf.broker.custom.cassandra.CassandraExistingEndpointBean;
import de.evoila.cf.broker.custom.cassandra.CassandraUtils;
import de.evoila.cf.broker.exception.PlatformException;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.UsernamePasswordCredential;
import de.evoila.cf.broker.repository.PlatformRepository;
import de.evoila.cf.broker.service.availability.ServicePortAvailabilityVerifier;
import de.evoila.cf.cpi.bosh.CredentialConstants;
import de.evoila.cf.security.credentials.CredentialStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author Johannes Hiemer.
 */
@Service
@ConditionalOnBean(CassandraExistingEndpointBean.class)
public class CassandraExistingServiceFactory extends ExistingServiceFactory {


    private CassandraCustomImplementation cassandraCustomImplementation;

    private CredentialStore credentialStore;

    public CassandraExistingServiceFactory(PlatformRepository platformRepository,
                                           CassandraCustomImplementation cassandraCustomImplementation,
                                           ServicePortAvailabilityVerifier portAvailabilityVerifier,
                                           CassandraExistingEndpointBean cassandraExistingEndpointBean,
                                           CredentialStore credentialStore) {
        super(platformRepository, portAvailabilityVerifier, cassandraExistingEndpointBean);
        this.cassandraCustomImplementation = cassandraCustomImplementation;
        this.credentialStore = credentialStore;
    }

	@Override
    public void deleteInstance(ServiceInstance serviceInstance, Plan plan) throws PlatformException {
        credentialStore.deleteCredentials(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS);

        CassandraDbService cassandraDbService = cassandraCustomImplementation.connection(serviceInstance, plan, null);
        cassandraCustomImplementation.deleteDatabase(cassandraDbService, CassandraUtils.dbName(serviceInstance.getId()));
        cassandraCustomImplementation.closeConnection(cassandraDbService);
	}

    @Override
    public ServiceInstance getInstance(ServiceInstance serviceInstance, Plan plan) {
        return serviceInstance;
    }

    @Override
    public ServiceInstance createInstance(ServiceInstance serviceInstance, Plan plan, Map<String, Object> parameters) throws PlatformException {
        credentialStore.createUser(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS);
        UsernamePasswordCredential serviceInstanceUsernamePasswordCredential = credentialStore.getUser(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS);

        serviceInstance.setUsername(serviceInstanceUsernamePasswordCredential.getUsername());

        CassandraDbService cassandraDbService = cassandraCustomImplementation.connection(serviceInstance, plan, null);
        cassandraCustomImplementation.createDatabase(cassandraDbService, CassandraUtils.dbName(serviceInstance.getId()));
        cassandraCustomImplementation.closeConnection(cassandraDbService);

        return serviceInstance;
	}

}
