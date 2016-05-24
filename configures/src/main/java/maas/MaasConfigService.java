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
@Command(name = "maas-config-service", description = "Message-as-a-Service configuration service")
public class MaasConfigService {

    @Option(name = {"-s", "--openshiftUri"}, description = "Openshift URI")
    public URI openshiftUri = URI.create("https://localhost:8443");

    @Option(name = {"-n", "--namespace"}, description = "Openshift namespace", required = true)
    public String openshiftNamespace;

    @Option(name = {"-u", "--user"}, description = "Openshift username", required = true)
    public String openshiftUser;

    @Option(name = {"-t", "--token"}, description = "Openshift auth token", required = true)
    public String openshiftToken;

    @Option(name = {"-l", "--listenAddress"}, description = "AMQP listen address")
    public String listenAddress = "localhost";

    @Option(name = {"-p", "--port"}, description = "AMQP listen port")
    public int listenPort = 5672;

    public static void main(String [] args) {

        // Workaround since -h and --help doesn't really work with airlift :(
        if (args.length == 0 || "-h".equals(args[0]) || "--help".equals(args[0]))
        {
            System.out.println("Usage maas-config-service -s <openshift uri, e.g. https://localhost:8443> -n <namespace> -u <openshiftUser> -t <openshiftToken> -l <listenAddress> -p <listenPort>");
            System.exit(1);
        }

        MaasConfigService config = SingleCommand.singleCommand(MaasConfigService.class).parse(args);

        IClient client = new ClientFactory().create(config.openshiftUri.toASCIIString(), new NoopSSLCertificateCallback());
        client.setAuthorizationStrategy(new TokenAuthorizationStrategy(config.openshiftToken, config.openshiftUser));

        OpenshiftConfigMapDatabase database = new OpenshiftConfigMapDatabase(client, config.openshiftNamespace);
        database.start();

        AMQPServer server = new AMQPServer(config.listenAddress, config.listenPort, database);

        server.run();

    }
}
