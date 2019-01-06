package net.sitemorph.protostore;

import java.util.Collection;
import java.util.Iterator;

/**
 * Iterate over all fields in the data set. Just wrap up a regular iterator.
 *
 * @author damien@sitemorph.net
 */
public class AllDataIterator<T> implements CrudIterator<T> {

  private final Iterator<T> iterator;

  public AllDataIterator(Collection<T> data) {
    this.iterator = data.iterator();
  }

  @Override
  public T next() throws CrudException {
    return iterator.next();
  }

  @Override
  public boolean hasNext() throws CrudException {
    return iterator.hasNext();
  }

  @Override
  public void close() throws CrudException {
  }
}
