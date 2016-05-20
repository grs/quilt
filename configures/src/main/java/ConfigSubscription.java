import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import java.util.Map;

/**
 * @author lulf
 */
public class ConfigSubscription {
    private final ProtonSender sender;
    public ConfigSubscription(ProtonSender sender) {
        this.sender = sender;
    }

    public void notifyClient(Map<String, String> values) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(values));
        sender.send(message, delivery -> {
            System.out.println("Client has received update!");
        });
        System.out.println("Notified client on update");
    }
}
