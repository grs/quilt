package quilt.config.model;

import java.util.Collections;
import java.util.List;

/**
 * @author lulf
 */
public final class Config {
    private final List<Broker> brokerList;

    public Config(List<Broker> brokerList) {
        this.brokerList = Collections.unmodifiableList(brokerList);
    }

    public List<Broker> brokers() {
        return brokerList;
    }
}
