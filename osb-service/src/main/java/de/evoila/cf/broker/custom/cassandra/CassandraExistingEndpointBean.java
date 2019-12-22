package de.evoila.cf.broker.custom.cassandra;

import de.evoila.cf.broker.bean.impl.ExistingEndpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

@Service
@ConfigurationProperties(prefix = "existing.endpoint")
//@ConditionalOnMissingBean(CassandraExistingEndpointBean.class)
public class CassandraExistingEndpointBean extends ExistingEndpoint {

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
