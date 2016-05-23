package maas;

import io.vertx.proton.ProtonSender;
import model.ConfigSubscriber;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a configsubscription on the Message-as-a-Service config which is defined in json format.
 *
 * @author lulf
 */
public class MaasConfigSubscriber implements ConfigSubscriber {
    private static final Logger log = Logger.getLogger(MaasConfigSubscriber.class.getName());
    private final ProtonSender sender;
    public MaasConfigSubscriber(ProtonSender sender) {
        this.sender = sender;
    }

    @Override
    public void configUpdated(String name, String version, Map<String, String> values) {
        Message message = Message.Factory.create();
        message.setBody(new AmqpValue(values.get("json")));
        message.setContentType("application/json");
        sender.send(message, delivery -> {
            log.log(Level.FINE, "Client has received update");
        });
        log.log(Level.FINE, "Notified client on update");
    }
}
