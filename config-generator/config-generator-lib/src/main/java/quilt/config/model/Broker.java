package quilt.config.model;

/**
 * @author lulf
 */
public final class Broker {
    private final String address;
    private final boolean storeAndForward;
    private final boolean multicast;

    public Broker(String address, boolean storeAndForward, boolean multicast) {
        this.address = address;
        this.storeAndForward = storeAndForward;
        this.multicast = multicast;
    }

    public String getAddress() {
        return address;
    }

    public boolean storeAndForward() {
        return storeAndForward;
    }

    public boolean multicast() {
        return multicast;
    }
}
