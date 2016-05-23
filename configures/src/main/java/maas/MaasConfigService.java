package maas;

import amqp.AMQPServer;
import com.openshift.restclient.ClientFactory;
import com.openshift.restclient.IClient;
import com.openshift.restclient.NoopSSLCertificateCallback;
import com.openshift.restclient.authorization.TokenAuthorizationStrategy;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import openshift.OpenshiftConfigMapDatabase;

import javax.inject.Inject;
import java.net.URI;

/**
 * Main entrypoint for configuration service with arg parsing.
 *
 * @author lulf
 */
@Command(name = "maas-config-service", description = "Run configuration service")
public class MaasConfigService implements Runnable {

    @Option(name = {"-s", "--openshiftUri"}, description = "Openshift URI", required = true)
    public URI openshiftUri;

    @Option(name = {"-n", "--namespace"}, description = "Openshift namespace", required = true)
    public String openshiftNamespace;

    @Option(name = {"-u", "--user"}, description = "Openshift username", required = true)
    public String openshiftUser;

    @Option(name = {"-t", "--token"}, description = "Openshift auth token", required = true)
    public String openshiftToken;

    @Option(name = {"-l", "--listenAddress"}, description = "AMQP listen address", required = true)
    public String listenAddress;

    @Option(name = {"-p", "--port"}, description = "AMQP listen port")
    public int listenPort = 5672;

    public static void main(String [] args) {
        SingleCommand<MaasConfigService> command = SingleCommand.singleCommand(MaasConfigService.class)<Runnable> command = Cli.<Runnable>builder("maas-config-service")
                .withDescription("Message-as-a-Service configuration service")
                .withDefaultCommand(Help.class)
                .withCommands(MaasConfigService.class);
        MaasConfigService maas = command.parse(args);
        if (maas.helpOption.showHelpIfRequested()) {
            return;
        }

        IClient client = new ClientFactory().create(openshiftUri.toASCIIString(), new NoopSSLCertificateCallback());
        client.setAuthorizationStrategy(new TokenAuthorizationStrategy(openshiftToken, openshiftUser));

        OpenshiftConfigMapDatabase database = new OpenshiftConfigMapDatabase(client, openshiftNamespace);
        database.start();

        AMQPServer server = new AMQPServer(listenAddress, listenPort, database);

        server.run();

    }
}
