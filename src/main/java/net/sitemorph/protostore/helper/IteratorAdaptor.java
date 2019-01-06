package net.sitemorph.protostore.helper;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.MessageNotFoundException;

import com.google.protobuf.Message;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator adaptor that
 */
public class IteratorAdaptor<T extends Message> implements Iterator<T>, Closeable {

  private volatile CrudIterator<T> iterator;

  public IteratorAdaptor(CrudIterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    try {
      return iterator.hasNext();
    } catch (CrudException e) {
      throw new IteratorAdaptorException("Storage exception checking for " +
          "hasNext item", e);
    }
  }

  @Override
  public T next() {
    try {
      boolean hasNext = iterator.hasNext();
      if (!hasNext) {
        iterator.close();
        throw new MessageNotFoundException("Attempted to access next when " +
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

  @Override
  public void close() throws CrudException {
    iterator.close();
  }
}
