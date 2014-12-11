package net.sitemorph.protostore;

/**
 * Crud storage exception.
 *
 * @author damien@sitemorph.net
 */
public class CrudException extends Exception {

  public CrudException(String message, Throwable source) {
    super(message, source);
  }

  public CrudException(String message) {
    super(message);
  }
}
