import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;

/**
 * @author lulf
 */
public class TestClient {

    public static void main(String [] args) {
        ProtonClient client = ProtonClient.create(Vertx.vertx());
        client.connect(new ProtonClientOptions().setConnectTimeout(10000), "172.17.0.3", 5672, connectResult -> {
            //client.connect(new ProtonClientOptions().setConnectTimeout(10000), "172.30.87.111", 5672, connectResult -> {
            if (connectResult.succeeded()) {
                System.out.println("Connected'");
                ProtonConnection connection = connectResult.result();
                connection.open();
                System.out.println("Creating receiver");
                connection.createReceiver("maas").handler(((delivery, message) -> {
                    System.out.println("Received " + message.getBody().toString());
                })).open();
            } else {
                System.out.println("Connection failed: " + connectResult.cause().getMessage());
            }
        });

        while (true);
    }
}
