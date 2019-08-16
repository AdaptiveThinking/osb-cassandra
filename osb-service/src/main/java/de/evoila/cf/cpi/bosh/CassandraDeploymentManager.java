package de.evoila.cf.cpi.bosh;

import de.evoila.cf.broker.bean.BoshProperties;
import de.evoila.cf.broker.custom.cassandra.CassandraUtils;
import de.evoila.cf.broker.model.ServiceInstance;
import de.evoila.cf.broker.model.catalog.plan.Plan;
import de.evoila.cf.broker.model.credential.UsernamePasswordCredential;
import de.evoila.cf.broker.util.MapUtils;
import de.evoila.cf.cpi.bosh.deployment.DeploymentManager;
import de.evoila.cf.cpi.bosh.deployment.manifest.Manifest;
import de.evoila.cf.security.credentials.CredentialStore;
import de.evoila.cf.security.credentials.DefaultCredentialConstants;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Johannes Hiemer.
 */
public class CassandraDeploymentManager extends DeploymentManager {

    public static final String INSTANCE_GROUP = "cassandra";

    private CredentialStore credentialStore;

    CassandraDeploymentManager(BoshProperties boshProperties, Environment environment, CredentialStore credentialStore){
        super(boshProperties, environment);
        this.credentialStore = credentialStore;
    }

    @Override
    protected void replaceParameters(ServiceInstance serviceInstance, Manifest manifest, Plan plan,
                                     Map<String, Object> customParameters, boolean isUpdate) {
        HashMap<String, Object> properties = new HashMap<>();
        if (customParameters != null && !customParameters.isEmpty())
            properties.putAll(customParameters);

        if (!isUpdate) {
            Map<String, Object> cassandraManifestProperties = manifestProperties(INSTANCE_GROUP, manifest);

            HashMap<String, Object> cassandraExporter = (HashMap<String, Object>) cassandraManifestProperties.get("cassandra_exporter");
            HashMap<String, Object> backupAgent = (HashMap<String, Object>) cassandraManifestProperties.get("backup_agent");
            HashMap<String, Object> cassandra = (HashMap<String, Object>) cassandraManifestProperties.get(INSTANCE_GROUP);

            List<HashMap<String, Object>> adminUsers = (List<HashMap<String, Object>>) cassandra.get("admin_users");
            HashMap<String, Object> userProperties = adminUsers.get(0);
            UsernamePasswordCredential rootCredentials = credentialStore.createUser(serviceInstance,
                    CredentialConstants.SERVICE_CREDENTIALS, "service");

            log.debug("##### Service User Creation #####");
            log.debug("Username: " + rootCredentials.getUsername());
            log.debug("ValueName: " + CredentialConstants.SERVICE_CREDENTIALS);
            log.debug("ServiceInstanceId: " + serviceInstance.getId());
            log.debug("Password: " + rootCredentials.getPassword());
            log.debug("##### After Get #####");
            UsernamePasswordCredential tmp = credentialStore.getUser(serviceInstance, CredentialConstants.SERVICE_CREDENTIALS)
            log.debug("Username: " + tmp.getUsername());
            log.debug("ValueName: " + CredentialConstants.SERVICE_CREDENTIALS);
            log.debug("ServiceInstanceId: " + serviceInstance.getId());
            log.debug("Password: " + tmp.getPassword());

            userProperties.put("username", rootCredentials.getUsername());
            userProperties.put("password", rootCredentials.getPassword());
            userProperties.put("superuser", true);
            serviceInstance.setUsername(rootCredentials.getUsername());

            UsernamePasswordCredential exporterCredential = credentialStore.createUser(serviceInstance,
                    DefaultCredentialConstants.EXPORTER_CREDENTIALS);
            cassandraExporter.put("username", exporterCredential.getUsername());
            cassandraExporter.put("password", exporterCredential.getPassword());
            cassandraExporter.put("superuser", true);
            HashMap<String, Object> exporterProperties = adminUsers.get(1);
            exporterProperties.put("username", exporterCredential.getUsername());
            exporterProperties.put("password", exporterCredential.getPassword());
            exporterProperties.put("superuser", true);

            UsernamePasswordCredential backupAgentUsernamePasswordCredential = credentialStore.createUser(serviceInstance,
                    DefaultCredentialConstants.BACKUP_AGENT_CREDENTIALS);
            backupAgent.put("username", backupAgentUsernamePasswordCredential.getUsername());
            backupAgent.put("password", backupAgentUsernamePasswordCredential.getPassword());

            List<HashMap<String, Object>> backupUsers = (List<HashMap<String, Object>>) cassandra.get("backup_users");
            HashMap<String, Object> backupUserProperties = backupUsers.get(0);
            UsernamePasswordCredential backupUsernamePasswordCredential = credentialStore.createUser(serviceInstance,
                    DefaultCredentialConstants.BACKUP_CREDENTIALS);
            backupUserProperties.put("username", backupUsernamePasswordCredential.getUsername());
            backupUserProperties.put("password", backupUsernamePasswordCredential.getPassword());
            backupUserProperties.put("superuser", true);

            List<HashMap<String, Object>> users = (List<HashMap<String, Object>>) cassandra.get("users");
            HashMap<String, Object> defaultUserProperties = users.get(0);
            UsernamePasswordCredential defaultUsernamePasswordCredential = credentialStore.createUser(serviceInstance,
                    CredentialConstants.USER_CREDENTIALS);
            defaultUserProperties.put("username", defaultUsernamePasswordCredential.getUsername());
            defaultUserProperties.put("password", defaultUsernamePasswordCredential.getPassword());

            List<String> databaseUsers = new ArrayList<>();
            databaseUsers.add(defaultUsernamePasswordCredential.getUsername());

            List<Map<String, Object>> databases = new ArrayList<>();
            Map<String, Object> database = new HashMap<>();
            database.put("name", CassandraUtils.dbName(serviceInstance.getId()));
            database.put("class", "SimpleStrategy");
            database.put("durable_writes", true);
            database.put("users", databaseUsers);
            databases.add(database);
            cassandra.put("databases", databases);

        } else if (isUpdate && customParameters != null && !customParameters.isEmpty()) {
            for (Map.Entry parameter : customParameters.entrySet()) {
                Map<String, Object> manifestProperties = manifestProperties(parameter.getKey().toString(), manifest);

                if (manifestProperties != null)
                    MapUtils.deepMerge(manifestProperties, customParameters);
            }

        }

        this.updateInstanceGroupConfiguration(manifest, plan);
    }

    private Map<String, Object> manifestProperties(String instanceGroup, Manifest manifest) {
        return manifest
                .getInstanceGroups()
                .stream()
                .filter(i -> {
                    if (i.getName().equals(instanceGroup))
                        return true;
                    return false;
                }).findFirst().get().getProperties();
    }
}
