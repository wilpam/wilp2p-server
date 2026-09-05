package wilpam.wilp2p;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ProjectPropertiesLookup implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectPropertiesLookup.class);
    private static final String CONFIG_FILE = "projects.yaml";

    private final Path configPath;
    private final WatchService watchService;
    private final Thread watcherThread;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Map<String, ProjectProperties> projects;

    public ProjectPropertiesLookup() {
        this(Path.of(CONFIG_FILE));
    }

    public ProjectPropertiesLookup(Path configPath) {
        this.configPath = Objects.requireNonNull(configPath).toAbsolutePath().normalize();
        this.projects = loadProjects();
        WatchService createdWatchService = null;
        try {
            createdWatchService = FileSystems.getDefault().newWatchService();
            this.configPath.getParent().register(
                    createdWatchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (IOException | RuntimeException e) {
            if (createdWatchService != null) {
                try {
                    createdWatchService.close();
                } catch (IOException closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw new IllegalStateException("Failed to watch " + configPath, e);
        }
        watchService = createdWatchService;
        watcherThread = new Thread(this::watchForChanges, "projects.yaml-watcher");
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    public boolean isRegistered(ProjectIdentifier identifier) {
        return projects.containsKey(identifier.key());
    }

    public ProjectProperties properties(ProjectIdentifier identifier) {
        ProjectProperties properties = projects.get(identifier.key());
        if (properties == null) {
            throw new IllegalStateException("Unknown project: " + identifier.key());
        }
        return properties;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            watchService.close();
        } catch (IOException e) {
            LOGGER.warn("Failed to close project properties watcher", e);
        }
        watcherThread.interrupt();
    }

    private Map<String, ProjectProperties> loadProjects() {
        if (!Files.exists(configPath)) {
            throw new IllegalStateException(
                    "Missing config file: projects.yaml (expected in working directory: "
                            + Path.of("").toAbsolutePath() + ")");
        }
        try (Reader reader = Files.newBufferedReader(configPath)) {
            Object loaded = new Yaml().load(reader);
            if (!(loaded instanceof Map<?, ?> map)) {
                throw new IllegalStateException("projects.yaml must contain a map of projects");
            }
            Map<String, ProjectProperties> parsed = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() instanceof String key && entry.getValue() instanceof Map<?, ?> properties) {
                    parsed.put(key, ProjectProperties.from(properties));
                }
            }
            return Collections.unmodifiableMap(parsed);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load projects.yaml", e);
        }
    }

    private void watchForChanges() {
        while (!closed.get()) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException e) {
                if (!closed.get()) {
                    Thread.currentThread().interrupt();
                }
                return;
            } catch (ClosedWatchServiceException e) {
                return;
            }

            boolean configChanged = false;
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.OVERFLOW) { // The best thing we can do here is just guess that we've changed
                    configChanged = true;
                    continue;
                }
                if (event.context() instanceof Path changed
                        && changed.equals(configPath.getFileName())) {
                    configChanged = true;
                }
            }
            if (!key.reset()) {
                return;
            }
            if (configChanged) {
                reloadAfterChange();
            }
        }
    }

    private void reloadAfterChange() {
        try {
            projects = loadProjects();
            LOGGER.info("Automatically updated project properties from {}", configPath);
        } catch (RuntimeException e) {
            LOGGER.warn(
                    "Could not automatically update project properties from {}; keeping the previous configuration",
                    configPath,
                    e);
        }
    }
}
