import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author lulf
 */
public class TestExample {
    public static void main(String [] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();

        ProtonServer server = ProtonServer.create(vertx);
        ExecutorService executor = Executors.newFixedThreadPool(10);

        // Connect, then use the event loop thread to process things thereafter
        server.connectHandler(connection -> {
            System.out.println("COnnect requested");
            connection.setContainer("my-container/server-id");
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

            connection.receiverOpenHandler(receiver -> {
                System.out.println("Receiver opened");
            }).open();

            connection.senderOpenHandler(sender -> {
                sender.setSource(sender.getRemoteSource());
                sender.open();
                Message message = Message.Factory.create();
                message.setBody(new AmqpValue("hello"));
                sender.send(message);
            });
        }).listen(5672, "localhost");

        ProtonClient client = ProtonClient.create(vertx);
        // Connect, then use the event loop thread to process things thereafter
        client.connect("localhost", 5672, connectResult -> {
            if (connectResult.succeeded()) {
                System.out.println("Connected'");
                ProtonConnection connection = connectResult.result();
                connection.setContainer("my-container/client-id").open();
                System.out.println("Creating receiver");
                connection.createReceiver("myQueue").handler(((delivery, message) -> {
                    System.out.println("Received " + message.toString());
                })).open();
                // Create senders, receivers etc..
            }
        });

        while (true) {
            Thread.sleep(1000);
        }
    }
}
