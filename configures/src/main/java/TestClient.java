import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;

/**
 * @author lulf
 */
public class TestClient {

    public static void main(String [] args) {
        ProtonClient client = ProtonClient.create(Vertx.vertx());
        client.connect("localhost", 12345, connectResult -> {
            if (connectResult.succeeded()) {
                System.out.println("Connected'");
                ProtonConnection connection = connectResult.result();
                connection.setContainer("bazmap").open();
                System.out.println("Creating receiver");
                connection.createReceiver("bazmap").handler(((delivery, message) -> {
                    System.out.println("Received " + message.toString());
                })).open();
            }
        });

        while (true);
    }
}
