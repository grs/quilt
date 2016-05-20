import com.openshift.restclient.IClient;
import com.openshift.restclient.model.IResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a ConfigMap that can be read and changed.
 *
 * @author lulf
 */
public class ConfigMap {
    private final String name;
    private String version;
    private Map<String, String> values;
    private final List<ConfigSubscription> subscriptionList = new ArrayList<>();

    public ConfigMap(String name, String version, Map<String, String> values) {
        this.name = name;
        this.version = version;
        this.values = values;
    }

    public synchronized void addSubscription(ConfigSubscription subscription) {
        subscriptionList.add(subscription);
        subscription.notifyClient(values);
    }

    public synchronized void configUpdated(String resourceVersion, Map<String, String> data) {
        version = resourceVersion;
        values = data;
        subscriptionList.stream().forEach(subscription -> {
            subscription.notifyClient(values);
        });
    }
}
