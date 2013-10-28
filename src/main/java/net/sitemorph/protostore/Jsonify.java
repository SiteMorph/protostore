package net.sitemorph.protostore;

import com.google.protobuf.Message;
import protobuf.codec.Codec;
import protobuf.codec.json.JsonCodec;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

/**
 * Factory and interface for json converter implementations.
 *
 * @author dak
 */
public class Jsonify <M extends Message> {

  private static Codec codec;

  static {
    codec = new JsonCodec();
  }

  public static String toJson(Message message) {
    StringWriter out = new StringWriter(256);
    try {
      codec.fromMessage(message, out);
      return out.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException("IO Error on message serialisation", e);
    }
  }

  public static Message toMessage(Class messageClass, String json) {
    try {
      return codec.toMessage(messageClass, new StringReader(json));
    } catch (IOException e) {
      throw new RuntimeException("IO Error on message deserialisation: " +
          json, e);
    }
  }
}
