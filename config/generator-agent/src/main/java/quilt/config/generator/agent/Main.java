package quilt.config.generator.agent;

import io.vertx.core.impl.FileResolver;

import java.io.IOException;

/**
 * @author lulf
 */
public class Main {

    public static void main(String args[]) {
        try {
            // Kind of a hack
            System.setProperty(FileResolver.CACHE_DIR_BASE_PROP_NAME, "/tmp/vert.x");

            GeneratorAgent service = new GeneratorAgent(GeneratorAgentOptions.fromEnv(System.getenv()));
            service.run();
        } catch (IllegalArgumentException e) {
            System.out.println(String.format("Unable to parse arguments: %s", e.getMessage()));
            System.exit(1);
        } catch (IOException e) {
            System.out.println("Error creating generator agent: " + e.getMessage());
            System.exit(1);
        }
    }
}
