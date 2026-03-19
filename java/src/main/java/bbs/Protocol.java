package bbs;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Protocol helpers: MessagePack serialization / deserialization.
 * All messages are Maps with at least "type" (String) and "timestamp" (double).
 */
public final class Protocol {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Protocol() {}

    // ── Timestamp ──────────────────────────────────────────────────────────────

    public static double now() {
        return Instant.now().toEpochMilli() / 1000.0;
    }

    public static String fmtTime(double ts) {
        long millis = (long) (ts * 1000);
        LocalDateTime ldt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        return ldt.format(FMT);
    }

    // ── Encoding ───────────────────────────────────────────────────────────────

    /**
     * Encode a Map<String, Object> to MessagePack bytes.
     * Supported value types: String, Double/Float, Long/Integer, List<String>, null
     */
    public static byte[] pack(Map<String, Object> msg) throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(msg.size());
            for (Map.Entry<String, Object> entry : msg.entrySet()) {
                packer.packString(entry.getKey());
                packValue(packer, entry.getValue());
            }
            return packer.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private static void packValue(MessageBufferPacker packer, Object value) throws IOException {
        if (value == null) {
            packer.packNil();
        } else if (value instanceof String s) {
            packer.packString(s);
        } else if (value instanceof Double d) {
            packer.packDouble(d);
        } else if (value instanceof Float f) {
            packer.packFloat(f);
        } else if (value instanceof Long l) {
            packer.packLong(l);
        } else if (value instanceof Integer i) {
            packer.packInt(i);
        } else if (value instanceof List<?> list) {
            packer.packArrayHeader(list.size());
            for (Object item : list) {
                packValue(packer, item);
            }
        } else {
            // Fallback: toString
            packer.packString(value.toString());
        }
    }

    // ── Decoding ───────────────────────────────────────────────────────────────

    /**
     * Decode MessagePack bytes into a Map<String, Object>.
     */
    public static Map<String, Object> unpack(byte[] data) throws IOException {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data)) {
            return unpackMap(unpacker);
        }
    }

    private static Map<String, Object> unpackMap(MessageUnpacker unpacker) throws IOException {
        int size = unpacker.unpackMapHeader();
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String key = unpacker.unpackString();
            Value val = unpacker.unpackValue();
            map.put(key, valueToJava(val));
        }
        return map;
    }

    private static Object valueToJava(Value val) {
        return switch (val.getValueType()) {
            case NIL     -> null;
            case BOOLEAN -> val.asBooleanValue().getBoolean();
            case INTEGER -> val.asIntegerValue().toLong();
            case FLOAT   -> val.asFloatValue().toDouble();
            case STRING  -> val.asStringValue().asString();
            case ARRAY -> {
                var arr = val.asArrayValue();
                var list = new java.util.ArrayList<Object>(arr.size());
                for (Value v : arr) list.add(valueToJava(v));
                yield list;
            }
            case MAP -> {
                var m = val.asMapValue();
                var result = new HashMap<String, Object>();
                for (var entry : m.entrySet()) {
                    result.put(entry.getKey().asStringValue().asString(), valueToJava(entry.getValue()));
                }
                yield result;
            }
            default -> val.toString();
        };
    }

    // ── Message builders ───────────────────────────────────────────────────────

    public static Map<String, Object> loginReq(String username) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "LOGIN");
        m.put("timestamp", now());
        m.put("username", username);
        return m;
    }

    public static Map<String, Object> listChannelsReq() {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "LIST_CHANNELS");
        m.put("timestamp", now());
        return m;
    }

    public static Map<String, Object> createChannelReq(String channelName) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", "CREATE_CHANNEL");
        m.put("timestamp", now());
        m.put("channel_name", channelName);
        return m;
    }

    public static Map<String, Object> response(String type, Map<String, Object> extras) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", type);
        m.put("timestamp", now());
        if (extras != null) m.putAll(extras);
        return m;
    }
}
