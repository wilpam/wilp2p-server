package wilpam.wilp2p;

/// namespace:project
public record ProjectIdentifier(String namespace, String project) {
    public String key() {
        return namespace + ":" + project;
    }
}
