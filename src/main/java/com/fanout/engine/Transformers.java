package com.fanout.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;

public class Transformers {
    private static final ObjectMapper M = new ObjectMapper();

    public static Transformer forSink(SinkType type) {
        return switch (type) {
            case REST -> item -> M.writeValueAsBytes(item.record); // JSON bytes
            case GRPC -> item -> ("PROTOBUF_SIM:" + item.record.toString()).getBytes(StandardCharsets.UTF_8);
            case MQ -> item -> {
                // XML mock
                String xml = "<record id=\"" + item.id + "\"><data>" +
                        escapeXml(item.record.toString()) + "</data></record>";
                return xml.getBytes(StandardCharsets.UTF_8);
            };
            case WIDECOLUMN -> item -> {
                // Mock Avro/CQL Map-style
                ObjectNode obj = M.createObjectNode();
                obj.put("id", item.id);
                obj.set("payload", item.record);
                obj.put("format", "WIDECOLUMN_SIM");
                return M.writeValueAsBytes(obj);
            };
        };
    }

    private static String escapeXml(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }
}
