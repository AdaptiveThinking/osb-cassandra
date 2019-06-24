package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.model.DashboardClient;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.repository.PlatformRepository;
import de.evoila.cf.broker.service.CatalogService;
import de.evoila.cf.broker.service.availability.ServicePortAvailabilityVerifier;
import de.evoila.cf.security.credentials.CredentialStore;
import io.bosh.client.deployments.Deployment;
import io.bosh.client.errands.ErrandSummary;
import io.bosh.client.vms.Vm;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import rx.Observable;

import java.util.List;
import java.util.Optional;

/**
 * @author Johannes Hiemer.
 */
@Service
@ConditionalOnBean(BoshProperties.class)
public class CassamdraBoshPlatformService extends BoshPlatformService {

    private static final int defaultPort = 9042;

    private CredentialStore credentialStore;

    CassamdraBoshPlatformService(PlatformRepository repository, CatalogService catalogService,
                                 ServicePortAvailabilityVerifier availabilityVerifier,
                                 BoshProperties boshProperties, Optional<DashboardClient> dashboardClient,
                                 Environment environment, CredentialStore credentialStore) {
        super(repository, catalogService,
                availabilityVerifier, boshProperties,
                dashboardClient, new CassandraDeploymentManager(boshProperties, environment, credentialStore));
        this.credentialStore = credentialStore;
    }

    public void runCreateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> errands) {}

    protected void runUpdateErrands(ServiceInstance instance, Plan plan, Deployment deployment, Observable<List<ErrandSummary>> errands) {}

    protected void runDeleteErrands(ServiceInstance instance, Deployment deployment, Observable<List<ErrandSummary>> errands) {}

    @Override
    protected void updateHosts(ServiceInstance serviceInstance, Plan plan, Deployment deployment) {
        List<Vm> vms = super.getVms(serviceInstance);
        serviceInstance.getHosts().clear();

        vms.forEach(vm -> serviceInstance.getHosts().add(super.toServerAddress(vm, defaultPort)));
    }

    @Override
    public void postDeleteInstance(ServiceInstance serviceInstance) {
        credentialStore.deleteCredentials(serviceInstance.getId(), CredentialConstants.SERVICE_CREDENTIALS);
    }

}
