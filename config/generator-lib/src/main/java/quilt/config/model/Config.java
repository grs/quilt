package quilt.config.model;

import com.openshift.restclient.images.DockerImageURI;
import com.sun.corba.se.pept.broker.Broker;

import java.util.Collection;
import java.util.Collections;

/**
 * @author lulf
 */
public final class Config {
    private final Collection<Destination> destinationList;
    private final BrokerProperties brokerProperties;

    public Config(Collection<Destination> destinationList, BrokerProperties brokerProperties) {
        this.destinationList = Collections.unmodifiableCollection(destinationList);
        this.brokerProperties = brokerProperties;
    }

    public Collection<Destination> destinations() {
        return destinationList;
    }

    public BrokerProperties brokerProperties() {
        return brokerProperties;
    }
}
