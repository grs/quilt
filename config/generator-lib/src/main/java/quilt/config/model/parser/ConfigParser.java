package quilt.config.model.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openshift.restclient.images.DockerImageURI;
import quilt.config.model.BrokerProperties;
import quilt.config.model.Config;
import quilt.config.model.Destination;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author lulf
 */
public class ConfigParser {
    private final ObjectMapper mapper = new ObjectMapper();

    public Config parse(Reader config) throws IOException {
        JsonNode root = mapper.readTree(config);
        return parse(root);
    }

    public Config parse(JsonNode root) throws IOException {
        Collection<Destination> destinations = parseDestinations(root.get("destinations"));

        BrokerProperties.Builder propsBuilder = new BrokerProperties.Builder();
        ifExists(root, "brokerProperties", props -> parseBrokerProperties(propsBuilder, props));

        return new Config(destinations, propsBuilder.build());
    }

    private BrokerProperties.Builder parseBrokerProperties(BrokerProperties.Builder builder, JsonNode json) {
        ifExists(json, "brokerImage", f -> builder.brokerImage(new DockerImageURI(f.asText())));
        ifExists(json, "brokerMounts", f -> builder.brokerMounts(readVolumeMount(f)));
        ifExists(json, "brokerPort", f -> builder.brokerPort(f.asInt()));

        ifExists(json, "routerImage", f -> builder.routerImage(new DockerImageURI(f.asText())));
        ifExists(json, "routerPort", f -> builder.routerPort(f.asInt()));
        return builder;
    }

    public static List<String> readVolumeMount(JsonNode mounts) {
        List<String> list = new ArrayList<>();
        mounts.forEach(elem -> list.add(elem.asText()));
        return list;
    }

    private static void ifExists(JsonNode json, String fieldName, Function<JsonNode, BrokerProperties.Builder> method) {
        if (json.has(fieldName)) {
            method.apply(json.get(fieldName));
        }
    }

    private Collection<Destination> parseDestinations(JsonNode destinations) {
        Iterator<Map.Entry<String, JsonNode>> it = destinations.fields();

        List<Destination> destinationList = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> entry = it.next();
            Destination destination = new Destination(
                    entry.getKey(),
                    entry.getValue().get("store_and_forward").asBoolean(),
                    entry.getValue().get("multicast").asBoolean());
            destinationList.add(destination);
        }
        return destinationList;
    }
}
