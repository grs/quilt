package quilt.config.admin;

import com.openshift.restclient.model.IReplicationController;
import quilt.config.generator.ConfigGenerator;
import quilt.config.model.BrokerProperties;
import quilt.config.model.Destination;
import quilt.config.model.LabelKeys;
import quilt.config.openshift.OpenshiftClient;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The {@link BrokerManager} maintains the number of broker replication controllers to be consistent with the number of destinations in config that require store_and_forward.
 *
 * @author lulf
 */
public class BrokerManager {
    private static final Logger log = Logger.getLogger(BrokerManager.class.getName());

    private final OpenshiftClient openshiftClient;
    private final ConfigGenerator generator;

    public BrokerManager(OpenshiftClient openshiftClient, ConfigGenerator generator) {
        this.openshiftClient = openshiftClient;
        this.generator = generator;
    }

    public void destinationsUpdated(Collection<Destination> destinations, BrokerProperties properties) {
        List<IReplicationController> currentBrokers = openshiftClient.listBrokers();
        log.log(Level.INFO, "Brokers got updated to " + destinations.size() + " destinations, we have " + currentBrokers.size() + " destinations: " + currentBrokers.stream().map(IReplicationController::getName).toString());
        createBrokers(currentBrokers, destinations, properties);
        deleteBrokers(currentBrokers, destinations, properties);
        updateBrokers(currentBrokers, destinations, properties);
    }

    private void createBrokers(Collection<IReplicationController> currentBrokers, Collection<Destination> newDestinations, BrokerProperties properties) {
        newDestinations.stream()
                .filter(broker -> !currentBrokers.stream().filter(controller -> broker.address().equals(controller.getLabels().get(LabelKeys.ADDRESS))).findAny().isPresent())
                .map(dest -> generator.generateBroker(dest, properties))
                .forEach(openshiftClient::createBroker);
    }

    private void deleteBrokers(Collection<IReplicationController> currentBrokers, Collection<Destination> newDestinations, BrokerProperties properties) {
        currentBrokers.stream()
                .filter(controller -> !newDestinations.stream().filter(broker -> broker.address().equals(controller.getLabels().get(LabelKeys.ADDRESS))).findAny().isPresent())
                .forEach(openshiftClient::deleteBroker);
    }

    private void updateBrokers(Collection<IReplicationController> currentBrokers, Collection<Destination> newDestinations, BrokerProperties properties) {
        newDestinations.stream()
                .filter(broker -> currentBrokers.stream().filter(controller -> broker.address().equals(controller.getLabels().get(LabelKeys.ADDRESS))).findAny().isPresent())
                .map(dest -> generator.generateBroker(dest, properties))
                .forEach(this::brokerModified);
    }

    private void brokerModified(IReplicationController controller) {
        IReplicationController oldController = openshiftClient.getBroker(controller.getName());
        oldController.setContainers(controller.getContainers());
        oldController.setReplicas(controller.getReplicas());
        oldController.setReplicaSelector(controller.getReplicaSelector());

        for (Map.Entry<String, String> label : controller.getLabels().entrySet()) {
            oldController.addLabel(label.getKey(), label.getValue());
        }
        openshiftClient.updateBroker(oldController);
    }
}
