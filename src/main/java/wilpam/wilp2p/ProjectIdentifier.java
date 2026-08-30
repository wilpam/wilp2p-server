package wilpam.wilp2p;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/// namespace:project
public record ProjectIdentifier(String namespace, String project) {
    public record ProjectProperties(
            boolean showRoomList,
            int maxPlayers
    ) {
        private static ProjectProperties from(Map<?, ?> config) {
            boolean showRoomList = Boolean.TRUE.equals(config.get("showRoomList"));
            int maxPlayers = -1;
            Object value = config.get("maxPlayers");
            if (value instanceof Number n) {
                maxPlayers = n.intValue();
            } else if (value != null && !String.valueOf(value).isEmpty()) {
                throw new IllegalStateException("maxPlayers must be a number, got: " + value);
            }
            return new ProjectProperties(showRoomList, maxPlayers);
        }
    }

    private static final String CONFIG_FILE = "projects.yaml";

    private static Map<String, Object> loadProjects() {
        Path path = Path.of(CONFIG_FILE);
        if (!Files.exists(path)) {
            throw new IllegalStateException(
                    "Missing config file: projects.yaml (expected in working directory: "
                            + Path.of("").toAbsolutePath() + ")");
        }
        try {
            Object loaded = new Yaml().load(Files.newBufferedReader(path));
            if (!(loaded instanceof Map<?, ?> map)) {
                throw new IllegalStateException("projects.yaml must contain a map of projects");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> projects = (Map<String, Object>) map;
            return projects;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load projects.yaml", e);
        }
    }

    public boolean isRegistered() {
        return loadProjects().get(key()) instanceof Map;
    }

    public ProjectProperties properties() {
        Object value = loadProjects().get(key());
        if (!(value instanceof Map<?, ?> properties)) {
            throw new IllegalStateException("Unknown project: " + key());
        }
        return ProjectProperties.from(properties);
    }

    private String key() {
        return namespace + ":" + project;
    }
}