package amqp;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonSession;
import maas.MaasConfigSubscriber;
import model.ConfigMapDatabase;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * AMQP server endpoint that handles connections to the service and propagates config.
 *
 * @author lulf
 */
public class AMQPServer {
    private static final Logger log = Logger.getLogger(AMQPServer.class.getName());

    // TODO: Should move somewhere to make it more generic
    private static final String MAAS_CONFIG = "maas";
    private final ConfigMapDatabase database;
    private final ProtonServer server;
    private final String hostname;
    private final int port;


    public AMQPServer(String hostname, int port, ConfigMapDatabase database)
    {
        this.hostname = hostname;
        this.port = port;
        this.database = database;
        this.server = ProtonServer.create(Vertx.vertx());

        server.connectHandler(this::connectHandler);
    }

    private void connectHandler(ProtonConnection connection) {
        connection.setContainer("server");
        connection.openHandler(conn -> {
            log.log(Level.INFO, "Connection opened");
        }).closeHandler(conn -> {
            connection.close();
            connection.disconnect();
            log.log(Level.INFO, "Connection closed");
        }).disconnectHandler(protonConnection -> {
            connection.disconnect();
            log.log(Level.INFO, "Disconnected");
        }).open();

        connection.sessionOpenHandler(ProtonSession::open);
        connection.senderOpenHandler(this::senderOpenHandler);
    }

    private void senderOpenHandler(ProtonSender sender) {
        sender.setSource(sender.getRemoteSource());
        sender.open();
        database.subscribe(MAAS_CONFIG, new MaasConfigSubscriber(sender));
    }

    public void run() {
        log.log(Level.INFO, String.format("Starting server on %s:%d", hostname, port));
        server.listen(port, hostname);
    }
}
