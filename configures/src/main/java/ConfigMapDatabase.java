import com.openshift.restclient.IClient;
import com.openshift.restclient.IOpenShiftWatchListener;
import com.openshift.restclient.IWatcher;
import com.openshift.restclient.ResourceKind;
import com.openshift.restclient.model.IConfigMap;
import com.openshift.restclient.model.IResource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Watches a ConfigMapList.
 *
 * @author lulf
 */
public class ConfigMapDatabase implements IOpenShiftWatchListener, AutoCloseable {
    private final IClient restClient;
    private final String namespace;
    private IWatcher watcher = null;

    private final Map<String, ConfigMap> configMapMap = new ConcurrentHashMap<>();

    public ConfigMapDatabase(IClient restClient, String namespace) {
        this.restClient = restClient;
        this.namespace = namespace;
    }

    public void start() {
        System.out.println("Starting database");
        this.watcher = restClient.watch(namespace, this, ResourceKind.CONFIG_MAP);
        System.out.println("Populated database");
    }

    @Override
    public void connected(List<IResource> resources) {
        System.out.println("Connected, got " + resources.size() + " resources");
        for (IResource resource : resources) {
            IConfigMap configMap = (IConfigMap) resource;
            String name = configMap.getName();
            String version = configMap.getResourceVersion();
            Map<String, String> values = configMap.getData();
            System.out.println(String.format("Found map %s version %s values %s", name, version, values));
            configMapMap.put(name, new ConfigMap(name, version, values));
        }
    }

    @Override
    public void disconnected() {
        System.out.println("disconnected");
        configMapMap.clear();
    }

    @Override
    public void received(IResource resource, ChangeType change) {
        IConfigMap configMap = (IConfigMap) resource;
        if (change.equals(ChangeType.DELETED)) {
            // TODO: Notify subscribers
            System.out.println("Subscription deleted");
            configMapMap.remove(configMap.getName());
        } else if (change.equals(ChangeType.ADDED)) {
            System.out.println("Subscription added");
            configMapMap.put(configMap.getName(), new ConfigMap(configMap.getName(), configMap.getResourceVersion(), configMap.getData()));
        } else if (change.equals(ChangeType.MODIFIED)) {
            System.out.println("Subscription modified");
            // TODO: Can we assume that it exists at this point?
            configMapMap.get(configMap.getName()).configUpdated(configMap.getResourceVersion(), configMap.getData());
        }
    }

    @Override
    public void error(Throwable err) {
        System.out.println(err.getMessage());
    }

    @Override
    public void close() throws Exception {
        if (this.watcher != null) {
            watcher.stop();
        }
    }

    public void addSubscription(String name, ConfigSubscription configSubscription) {
        ConfigMap configMap = configMapMap.get(name);
        if (configMap != null) {
            configMap.addSubscription(configSubscription);
        }
    }
}
