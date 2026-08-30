package wilpam.wilp2p;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import wilpam.json.deserialize.Deserializer;
import wilpam.json.obj.JsonArray;
import wilpam.json.obj.JsonBool;
import wilpam.json.obj.JsonNull;
import wilpam.json.obj.JsonNumber;
import wilpam.json.obj.JsonObject;
import wilpam.json.obj.JsonString;
import wilpam.json.obj.JsonType;
import wilpam.json.serialize.Serializer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class Wilp2pServer extends WebSocketServer {
    private static final String PROTOCOL_VERSION = "1.0.0";
    private static final int MAX_PASSWORD_ATTEMPTS = 3;
    private static final String PROJECT_IDENTIFIER_PATTERN = "[a-z0-9_]+:[a-z0-9_]+";
    private static final String ROOM_ID_PATTERN = "[A-Za-z0-9]+";
    private static final String ROOM_SEPARATOR = "\u0000";

    private static final JsonString TYPE_KEY = new JsonString("type");
    private static final JsonString DATA_KEY = new JsonString("data");
    private static final JsonString WILP2P_CID_KEY = new JsonString("wilp2p_cid");

    private enum State {
        AWAITING_START,
        STARTED,
        JOINING,
        IN_ROOM
    }

    private static final class ClientState {
        final WebSocket conn;
        State state = State.AWAITING_START;
        String project;
        Room room;
        int clientId = -1;
        int passwordAttemptsLeft;
        Room pendingJoin;

        ClientState(WebSocket conn) {
            this.conn = conn;
        }
    }

    private static final class Room {
        final String project;
        final String id;
        final String name;
        final String creator;
        final String password;
        final JsonType extra;
        final int maxPlayers;

        final Map<Integer, WebSocket> players = new LinkedHashMap<>();
        private int nextClientId = 0;

        Room(String project, String id, String name, String creator, String password, JsonType extra, int maxPlayers) {
            this.project = project;
            this.id = id;
            this.name = name;
            this.creator = creator;
            this.password = password;
            this.extra = extra;
            this.maxPlayers = maxPlayers;
        }

        boolean isPasswordProtected() {
            return password != null;
        }

        boolean isFull() {
            return maxPlayers >= 0 && players.size() >= maxPlayers;
        }

        int addPlayer(WebSocket conn) {
            int clientId = nextClientId++;
            players.put(clientId, conn);
            return clientId;
        }

        void removePlayer(WebSocket conn) {
            players.entrySet().removeIf(entry -> entry.getValue() == conn);
        }

        boolean isEmpty() {
            return players.isEmpty();
        }

        JsonObject toJson() {
            Map<JsonString, JsonType> fields = new LinkedHashMap<>();
            fields.put(new JsonString("id"), new JsonString(id));
            fields.put(new JsonString("password_protected"), new JsonBool(isPasswordProtected()));
            fields.put(new JsonString("name"), name == null ? new JsonNull() : new JsonString(name));
            fields.put(new JsonString("creator"), creator == null ? new JsonNull() : new JsonString(creator));
            fields.put(new JsonString("extra"), extra == null ? new JsonNull() : extra);
            fields.put(new JsonString("players"), new JsonNumber(String.valueOf(players.size())));
            fields.put(new JsonString("max_players"), new JsonNumber(String.valueOf(maxPlayers)));
            return new JsonObject(fields);
        }
    }

    private final Object lock = new Object();
    private final Map<WebSocket, ClientState> clients = new LinkedHashMap<>();
    private final Map<String, Room> rooms = new LinkedHashMap<>();

    public Wilp2pServer(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        synchronized (lock) {
            clients.put(conn, new ClientState(conn));
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        synchronized (lock) {
            ClientState state = clients.get(conn);
            if (state != null) {
                leaveRoom(state, "disconnected");
                clients.remove(conn);
            }
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        synchronized (lock) {
            ClientState state = clients.get(conn);
            if (state == null) {
                return;
            }
            if (state.state == State.IN_ROOM && state.room != null) {
                forwardText(state, message);
                return;
            }
            JsonType parsed;
            try {
                parsed = Deserializer.deserialize(message);
            } catch (Exception ex) {
                conn.close(1003, "invalid json");
                return;
            }
            if (parsed instanceof JsonObject(Map<JsonString, JsonType> map)
                    && map.get(TYPE_KEY) instanceof JsonString(String string)
                    && map.get(DATA_KEY) instanceof JsonObject data) {
                handleProtocolMessage(state, string, data);
            } else {
                conn.close(1003, "invalid message format");
            }
        }
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer message) {
        synchronized (lock) {
            ClientState state = clients.get(conn);
            if (state == null || state.state != State.IN_ROOM || state.room == null) {
                if (state != null && state.state != State.IN_ROOM) {
                    conn.close(1003, "binary not allowed during handshake");
                }
                return;
            }
            byte[] payload = new byte[message.remaining()];
            message.get(payload);
            ByteBuffer forwarded = ByteBuffer.allocate(4 + payload.length);
            forwarded.putInt(state.clientId);
            forwarded.put(payload);
            forwarded.flip();
            for (WebSocket other : state.room.players.values()) {
                if (other != conn && other.isOpen()) {
                    other.send(forwarded.duplicate());
                }
            }
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        // TODO: fix me
        ex.printStackTrace();
    }

    @Override
    public void onStart() {
        System.out.println("Server started!");
        setConnectionLostTimeout(100);
    }

    private void handleProtocolMessage(ClientState state, String type, JsonObject data) {
        switch (type) {
            case "start" -> handleStart(state, data);
            case "create_room" -> handleCreateRoom(state, data);
            case "join_room" -> handleJoinRoom(state, data);
            case "room_join_password" -> handleRoomJoinPassword(state, data);
            default -> {
            }
        }
    }

    private void handleStart(ClientState state, JsonObject data) {
        if (state.state != State.AWAITING_START) {
            state.conn.close(1003, "you've already started");
            return;
        }
        String id = stringField(data, "id");
        String version = stringField(data, "version");
        if (id == null || version == null) {
            return;
        }
        if (!id.matches(PROJECT_IDENTIFIER_PATTERN)) {
            state.conn.close(1003, String.format("'%s' is not a project identifier", id));
            return;
        }
        if (compareVersions(version, PROTOCOL_VERSION) > 0) {
            state.conn.close(1003, String.format("'%s' is too new for me", version));
            return;
        }
        int separator = id.indexOf(':');
        ProjectIdentifier identifier = new ProjectIdentifier(
                id.substring(0, separator), id.substring(separator + 1));
        if (!identifier.isRegistered()) {
            state.conn.close(1003, String.format("'%s' is not a registered project identifier", id));
            return;
        }
        state.project = id;
        state.state = State.STARTED;

        Map<String, JsonType> fields = new LinkedHashMap<>();
        fields.put("version", new JsonString(PROTOCOL_VERSION));
        if (identifier.properties().showRoomList()) {
            fields.put("rooms", roomsArray(id));
        }
        sendTyped(state.conn, "start_reply", dataOf(fields));
    }

    private void handleCreateRoom(ClientState state, JsonObject data) {
        if (state.state != State.STARTED || state.project == null) {
            state.conn.close(1003, "you need to initiate first with start");
            return;
        }
        String roomId = stringField(data, "id");
        if (roomId == null || !roomId.matches(ROOM_ID_PATTERN)) {
            state.conn.close(1003, String.format("'%s' is not a room id", roomId));
            return;
        }
        String key = roomKey(state.project, roomId);
        if (rooms.containsKey(key)) {
            sendTyped(state.conn, "room_create_failed",
                    dataOf(singleField("reason", new JsonString("room_already_created"))));
            return;
        }
        String password = stringField(data, "password");
        Room room = new Room(state.project, roomId,
                stringField(data, "name"),
                stringField(data, "creator"),
                password == null || password.isEmpty() ? null : password,
                fieldValue(data, "extra"),
                projectMaxPlayers(state.project));
        rooms.put(key, room);

        int clientId = room.addPlayer(state.conn);
        state.room = room;
        state.clientId = clientId;
        state.state = State.IN_ROOM;
        sendTyped(state.conn, "room_created", dataOf(singleField("id", new JsonNumber(String.valueOf(clientId)))));
    }

    private static int projectMaxPlayers(String projectId) {
        int separator = projectId.indexOf(':');
        ProjectIdentifier identifier = new ProjectIdentifier(
                projectId.substring(0, separator), projectId.substring(separator + 1));
        return identifier.properties().maxPlayers();
    }

    private void handleJoinRoom(ClientState state, JsonObject data) {
        if (state.state != State.STARTED || state.project == null) {
            state.conn.close(1003, "you need to initiate first with start");
            return;
        }
        String roomId = stringField(data, "id");
        if (roomId == null) {
            state.conn.close(1003, "you need to provide a room id");
            return;
        }
        Room room = rooms.get(roomKey(state.project, roomId));
        if (room == null) {
            sendTyped(state.conn, "room_join_failed",
                    dataOf(singleField("reason", new JsonString("room_does_not_exist"))));
            return;
        }
        if (room.isPasswordProtected()) {
            state.state = State.JOINING;
            state.pendingJoin = room;
            state.passwordAttemptsLeft = MAX_PASSWORD_ATTEMPTS;
            sendTyped(state.conn, "room_join_password_request",
                    dataOf(singleField("attempts", new JsonNumber(String.valueOf(MAX_PASSWORD_ATTEMPTS)))));
            return;
        }
        joinRoom(state, room);
    }

    private void handleRoomJoinPassword(ClientState state, JsonObject data) {
        if (state.state != State.JOINING || state.pendingJoin == null) {
            state.conn.close(1003, "you don't need to give a password");
            return;
        }
        Room room = state.pendingJoin;
        String password = stringField(data, "password");
        if (password != null && password.equals(room.password)) {
            state.pendingJoin = null;
            joinRoom(state, room);
            return;
        }
        state.passwordAttemptsLeft--;
        if (state.passwordAttemptsLeft <= 0) {
            state.pendingJoin = null;
            state.state = State.STARTED;
            sendTyped(state.conn, "room_join_failed",
                    dataOf(singleField("reason", new JsonString("too_many_password_attempts"))));
            return;
        }
        sendTyped(state.conn, "room_join_password_request",
                dataOf(singleField("attempts", new JsonNumber(String.valueOf(state.passwordAttemptsLeft)))));
    }

    private void joinRoom(ClientState state, Room room) {
        if (room.isFull()) {
            state.state = State.STARTED;
            sendTyped(state.conn, "room_join_failed",
                    dataOf(singleField("reason", new JsonString("room_full"))));
            return;
        }
        int clientId = room.addPlayer(state.conn);
        state.room = room;
        state.clientId = clientId;
        state.state = State.IN_ROOM;
        sendTyped(state.conn, "room_joined", dataOf(singleField("id", new JsonNumber(String.valueOf(clientId)))));
        broadcastClientEvent(room, "client_joined", clientId, state.conn);
    }

    private void leaveRoom(ClientState state, String reason) {
        Room room = state.room;
        if (room == null) {
            state.state = State.STARTED;
            state.clientId = -1;
            state.pendingJoin = null;
            return;
        }
        int clientId = state.clientId;
        room.removePlayer(state.conn);
        state.room = null;
        state.clientId = -1;
        state.state = State.STARTED;
        state.pendingJoin = null;
        if (room.isEmpty()) {
            rooms.remove(roomKey(room.project, room.id));
            return;
        }
        Map<String, JsonType> fields = new LinkedHashMap<>();
        fields.put("id", new JsonNumber(String.valueOf(clientId)));
        fields.put("reason", new JsonString(reason));
        String text = serialize(envelope("client_left", dataOf(fields)));
        for (WebSocket other : room.players.values()) {
            if (other.isOpen()) {
                other.send(text);
            }
        }
    }

    private void broadcastClientEvent(Room room, String type, int clientId, WebSocket except) {
        String text = serialize(envelope(type, dataOf(singleField("id", new JsonNumber(String.valueOf(clientId))))));
        for (WebSocket other : room.players.values()) {
            if (other != except && other.isOpen()) {
                other.send(text);
            }
        }
    }

    private void forwardText(ClientState state, String message) {
        JsonType parsed;
        try {
            parsed = Deserializer.deserialize(message);
        } catch (Exception ex) {
            propagateText(state.room, message, state.conn);
            return;
        }
        if (parsed instanceof JsonObject(Map<JsonString, JsonType> map)) {
            Map<JsonString, JsonType> fields = new LinkedHashMap<>(map);
            fields.put(WILP2P_CID_KEY, new JsonNumber(String.valueOf(state.clientId)));
            propagateText(state.room, serialize(new JsonObject(fields)), state.conn);
        } else {
            propagateText(state.room, message, state.conn);
        }
    }

    private void propagateText(Room room, String text, WebSocket except) {
        for (WebSocket other : room.players.values()) {
            if (other != except && other.isOpen()) {
                other.send(text);
            }
        }
    }

    private void sendTyped(WebSocket conn, String type, JsonObject data) {
        if (conn.isOpen()) {
            conn.send(serialize(envelope(type, data)));
        }
    }

    private JsonObject envelope(String type, JsonObject data) {
        Map<JsonString, JsonType> outer = new LinkedHashMap<>();
        outer.put(TYPE_KEY, new JsonString(type));
        outer.put(DATA_KEY, data);
        return new JsonObject(outer);
    }

    private JsonArray roomsArray(String project) {
        List<JsonType> list = new ArrayList<>();
        for (Room room : rooms.values()) {
            if (room.project.equals(project)) {
                list.add(room.toJson());
            }
        }
        return new JsonArray(list);
    }

    private static String roomKey(String project, String roomId) {
        return project + ROOM_SEPARATOR + roomId;
    }

    private static JsonObject dataOf(Map<String, JsonType> fields) {
        Map<JsonString, JsonType> data = new LinkedHashMap<>();
        for (Map.Entry<String, JsonType> entry : fields.entrySet()) {
            data.put(new JsonString(entry.getKey()), entry.getValue());
        }
        return new JsonObject(data);
    }

    private static Map<String, JsonType> singleField(String key, JsonType value) {
        Map<String, JsonType> fields = new LinkedHashMap<>();
        fields.put(key, value);
        return fields;
    }

    private static String stringField(JsonObject data, String key) {
        JsonType value = fieldValue(data, key);
        return value instanceof JsonString(String string) ? string : null;
    }

    private static JsonType fieldValue(JsonObject data, String key) {
        return data.map().get(new JsonString(key));
    }

    private static String serialize(JsonObject obj) {
        return Serializer.serialize(obj);
    }

    private static int compareVersions(String a, String b) {
        String[] left = a.split("\\.");
        String[] right = b.split("\\.");
        int length = Math.max(left.length, right.length);
        for (int i = 0; i < length; i++) {
            int x;
            int y;
            try {
                x = i < left.length ? Integer.parseInt(left[i]) : 0;
                y = i < right.length ? Integer.parseInt(right[i]) : 0;
            } catch (NumberFormatException e) {
                return 0;
            }
            if (x != y) {
                return Integer.compare(x, y);
            }
        }
        return 0;
    }
}