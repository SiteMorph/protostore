package net.sitemorph.protostore;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;

import java.util.List;

/**
 * General reader functions for accessing collections of messages using a simple
 * iterable interface.
 *
 * Additionally the iterables must close themselves as they consume resources.
 * This means that the factory must hold references to the claimed iterators via
 * register / return
 *
 * @author damien@sitemorph.net
 */
public class ReaderFactory<T extends Message> {

  private CrudFactory factory;
  private List<CrudReader> readerList = Lists.newArrayList();

  public ReaderFactory(CrudFactory factory) {
    this.factory = factory;
  }

  public CrudReader<T> getReader(T.Builder builder) throws CrudException {
    CrudStore<T> store = factory.getCrudStore(builder);
    CrudReader<T> reader = new CrudReader<T>(factory, builder, this);
    readerList.add(reader);
    return reader;
  }

  public void close() throws CrudException {
    for (CrudReader reader : readerList) {
      reader.close();
    }
    readerList.clear();
  }
}
