import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;

/**
 * @author lulf
 */
public class TestClient {

    public static void main(String [] args) {
        ProtonClient client = ProtonClient.create(Vertx.vertx());
        client.connect("localhost", 5672, connectResult -> {
            if (connectResult.succeeded()) {
                System.out.println("Connected'");
                ProtonConnection connection = connectResult.result();
                connection.open();
                System.out.println("Creating receiver");
                connection.createReceiver("maas").handler(((delivery, message) -> {
                    System.out.println("Received " + message.getBody().toString());
                })).open();
            }
        });

        while (true);
    }
}
