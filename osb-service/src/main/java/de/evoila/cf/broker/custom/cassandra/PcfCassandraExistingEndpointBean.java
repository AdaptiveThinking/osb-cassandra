package de.evoila.cf.broker.custom.cassandra;

import com.fasterxml.jackson.core.type.TypeReference;
import de.evoila.cf.broker.bean.impl.ExistingEndpoint;
import de.evoila.cf.broker.model.catalog.ServerAddress;
import de.evoila.cf.broker.util.ObjectMapperUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
@Profile("pcf")
@ConfigurationProperties(prefix = "existing.endpoint")
public class PcfCassandraExistingEndpointBean extends CassandraExistingEndpointBean {
    
    private String pcfHosts;
    private String datacenter;

    public String getPcfHosts() {
        return pcfHosts;
    }

    public void setPcfHosts(String pcfHosts) throws IOException {
        this.pcfHosts = pcfHosts;
    }


    public PcfCassandraExistingEndpointBean() {
        super();
    }

    public String getDatacenter() {
        return datacenter;
    }

    public void setDatacenter(String datacenter) {
        this.datacenter = datacenter;
    }

   @PostConstruct
   public void initHosts() throws Exception {
        List<String> pcfHostList = ObjectMapperUtils.getObjectMapper().readValue(this.pcfHosts, new TypeReference<List<String>>(){});
        for (String host : pcfHostList) {
            getHosts().add(new ServerAddress(getName(), host, getPort()));
        }
   }
}
