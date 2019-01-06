package net.sitemorph.protostore.helper;

import net.sitemorph.protostore.CrudException;

/**
 * Iterator adaptor functions have to throw runtime exceptions as they can't
 * be checked when adapting a storage iterator with crud exceptions to a java
 * iterable.
 *
 * @author damien@sitemorph.net
 */
public class IteratorAdaptorException extends RuntimeException {
  public IteratorAdaptorException(String message, CrudException e) {
    super(message, e);
  }
}
