import com.openshift.restclient.ClientFactory;
import com.openshift.restclient.IClient;
import com.openshift.restclient.NoopSSLCertificateCallback;
import com.openshift.restclient.authorization.TokenAuthorizationStrategy;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;

/**
 * @author lulf
 */
public class Main {
    public static void createSubscription(String name, ProtonSender sender, ConfigMapDatabase database) {
        database.addSubscription(name, new ConfigSubscription(sender));
        System.out.println("Requested sender open");
    }

    public static void main(String [] args) {
        IClient client = new ClientFactory().create("https://localhost:8443", new NoopSSLCertificateCallback());
        client.setAuthorizationStrategy(new TokenAuthorizationStrategy("oO6zu3c7oZE_y2D0dAnj6xILKKjsG6jEJDg76fAyjqk", "test"));
        ConfigMapDatabase database = new ConfigMapDatabase(client, "petproject");

        ProtonServer server = ProtonServer.create(Vertx.vertx());

        server.connectHandler(connection-> {
            connection.setContainer("server");
            connection.openHandler(conn -> {
                System.out.println("Client connected");
            }).closeHandler(conn -> {
                connection.close();
                connection.disconnect();
                System.out.println("Connection closed");
            }).disconnectHandler(protonConnection -> {
                System.out.println("Client disconnected");
                connection.disconnect();
            }).open();

            connection.sessionOpenHandler(session -> session.open());

            connection.senderOpenHandler(sender -> {
                sender.setSource(sender.getRemoteSource());
                sender.open();
                createSubscription(connection.getRemoteContainer(), sender, database);
            });
        }).listen(12345, "localhost");

        database.start();

        while (true);
    }
}
