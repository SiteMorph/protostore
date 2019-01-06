package net.sitemorph.protostore;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Get a collection of messages.
 *
 * @author damien@sitemorph.net
 */
public class CrudReader<T extends Message> implements Iterable<T> {

  private final CrudFactory storeFactory;
  private final T.Builder builder;
  private final ReaderFactory<T> factory;
  private List<IteratorAdaptor> closeMe = new ArrayList<>();
  private List<CrudStore> stores = new ArrayList<>();

  public CrudReader(CrudFactory storeFactory, T.Builder builder,
      ReaderFactory<T> readerFactory) {
    this.storeFactory = storeFactory;
    this.builder = builder;
    this.factory = readerFactory;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      CrudStore<T> store = storeFactory.getCrudStore(builder);
      stores.add(store);
      return new IteratorAdaptor<T>(store.read(builder), this);
    } catch (CrudException e) {
      throw new IteratorAdaptorException("Error getting iterator for " +
          "crud collection", e);
    }
  }

  /**
   * Close the current iterator set if they have not already been closed by their
   * own operations.
   *
   * @throws CrudException on close error
   */
  public void close() throws CrudException {
    for (IteratorAdaptor iterator : closeMe) {
      iterator.close();
    }
    closeMe.clear();
    for (CrudStore store : stores) {
      store.close();
    }
    stores.clear();
  }

  void closed(IteratorAdaptor adaptor) {
    closeMe.remove(adaptor);
  }
}
