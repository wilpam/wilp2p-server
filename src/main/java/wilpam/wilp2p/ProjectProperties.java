package wilpam.wilp2p;

import java.util.Map;

public record ProjectProperties(
        boolean showRoomList,
        int maxPlayers
) {
    static ProjectProperties from(Map<?, ?> config) {
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
