package net.sitemorph.protostore;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator adaptor that
 */
public class IteratorAdaptor<T> implements Iterator<T> {

  private volatile Reader reader;
  private volatile CrudIterator<T> iterator;
  private volatile boolean closed = false;

  public IteratorAdaptor(CrudIterator<T> iterator, Reader reader) {
    this.iterator = iterator;
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    try {
      boolean hasNext = iterator.hasNext();
      if (!hasNext && !closed) {
        iterator.close();
        closed = true;
        reader.closed(this);
      }
      return hasNext;
    } catch (CrudException e) {
      throw new IteratorAdaptorException("Storage exception checking for " +
          "hasNext item", e);
    }
  }

  @Override
  public T next() {
    try {
      boolean hasNext = iterator.hasNext();
      if (!hasNext && !closed) {
        iterator.close();
        closed = true;
        reader.closed(this);
        throw new NoSuchElementException("Attempted to access next when " +
            "none exists");
      }
      return iterator.next();
    } catch (CrudException e) {
      throw new IteratorAdaptorException("Storage error getting next item", e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove operation not supported " +
        "for crud backed iterator");
  }

  public void close() throws CrudException {
    if (!closed) {
      iterator.close();
      closed = true;
      reader.closed(this);
    }
  }
}
