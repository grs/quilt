package quilt.config.generator;

import com.openshift.internal.restclient.ResourceFactory;
import com.openshift.internal.restclient.model.Port;
import com.openshift.internal.restclient.model.ReplicationController;
import com.openshift.restclient.ResourceKind;
import com.openshift.restclient.images.DockerImageURI;
import com.openshift.restclient.model.IReplicationController;
import quilt.config.model.Broker;
import org.jboss.dmr.ModelNode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author lulf
 */
public class BrokerGenerator {
    private final ResourceFactory factory = new ResourceFactory(null);

    public IReplicationController generate(Broker broker) {
        ReplicationController controller = factory.create("v1", ResourceKind.REPLICATION_CONTROLLER);

        controller.setName("broker-controller");
        controller.setReplicas(1);
        controller.addLabel("role", "broker");
        controller.addLabel("address", broker.getAddress());
        controller.addTemplateLabel("role", "broker");
        controller.setReplicaSelector(Collections.singletonMap("role", "broker"));

        generateBroker(controller);
        generateConfigurationDaemon(controller, broker);
        generateDispatchRouter(controller);

        return controller;
    }

    private void generateBroker(ReplicationController controller) {
        Port amqpPort = new Port(new ModelNode());
        amqpPort.setContainerPort(5672);

        controller.addContainer(
                "broker",
                new DockerImageURI("gordons/qpidd:v4"),
                Collections.singleton(amqpPort),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private void generateConfigurationDaemon(ReplicationController controller, Broker broker) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("QUEUE_NAME", broker.getAddress());
        controller.addContainer(
                "configured",
                new DockerImageURI("gordons/configured:v9"),
                Collections.emptySet(),
                env,
                Collections.emptyList());
    }

    private void generateDispatchRouter(ReplicationController controller) {
        Port interRouterPort = new Port(new ModelNode());
        interRouterPort.setContainerPort(55672);

        controller.addContainer(
                "router",
                new DockerImageURI("gordons/qdrouterd:v4"),
                Collections.singleton(interRouterPort),
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
