package net.sitemorph.protostore;

/**
 * Message vector exception used to signal that an update will fail due to clock
 * vector state.
 *
 * @author damien@sitemorph.net
 */
public class MessageVectorException extends CrudException {
  public MessageVectorException(String message) {
    super(message);
  }

  public MessageVectorException(String message, Throwable source) {
    super(message, source);
  }
}
