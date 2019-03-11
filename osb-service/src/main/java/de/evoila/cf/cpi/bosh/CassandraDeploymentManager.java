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

            HashMap<String, Object> cassandra = (HashMap<String, Object>) cassandraManifestProperties.get(INSTANCE_GROUP);

            List<HashMap<String, Object>> users = (List<HashMap<String, Object>>) cassandra.get("users");
            HashMap<String, Object> userProperties = users.get(0);
            UsernamePasswordCredential usernamePasswordCredential = credentialStore.createUser(serviceInstance,
                    CredentialConstants.ROOT_CREDENTIALS);
            userProperties.put("username", usernamePasswordCredential.getUsername());
            userProperties.put("password", usernamePasswordCredential.getPassword());
            userProperties.put("superuser", true);

            List<Map<String, Object>> databases = new ArrayList<>();
            Map<String, Object> database = new HashMap<>();
            databases.add(database);
            database.put("name", CassandraUtils.dbName(serviceInstance.getId()));
            database.put("class", "SimpleStrategy");
            database.put("durable_writes", true);

            List<String> user = new ArrayList<>();
            user.add(usernamePasswordCredential.getUsername());
            database.put("users", user);

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
