package net.sitemorph.protostore;

/**
 * Indication that an update or other operation was attempted on a resource
 * message that was not found.
 */
public class MessageNotFound extends CrudException {
  public MessageNotFound(String message) {
    super(message);
  }

  public MessageNotFound(String message, Throwable cause) {
    super(message, cause);
  }
}
