package net.sitemorph.protostore;

import java.io.IOException;

/**
 * Crud storage exception.
 *
 * @author damien@sitemorph.net
 */
public class CrudException extends IOException {

  public CrudException(String message, Throwable source) {
    super(message, source);
  }

  public CrudException(String message) {
    super(message);
  }
}
