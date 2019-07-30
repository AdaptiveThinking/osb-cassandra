package de.evoila.cf.broker.custom.cassandra;

import de.evoila.cf.broker.bean.impl.ExistingEndpointBeanImpl;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!pcf")
@ConfigurationProperties(prefix = "existing.endpoint")
public class CassandraExistingEndpointBean extends ExistingEndpointBeanImpl {

    private String datacenter;

    public CassandraExistingEndpointBean() {
        super();
    }

    public String getDatacenter() {
        return datacenter;
    }

    public void setDatacenter(String datacenter) {
        this.datacenter = datacenter;
    }
}
