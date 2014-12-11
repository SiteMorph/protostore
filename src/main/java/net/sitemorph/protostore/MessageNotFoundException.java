package net.sitemorph.protostore;

/**
 * Indication that an update or other operation was attempted on a resource
 * message that was not found.
 */
public class MessageNotFoundException extends CrudException {
  public MessageNotFoundException(String message) {
    super(message);
  }

  public MessageNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
