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
  private List<Reader> readerList = Lists.newArrayList();

  private ReaderFactory(CrudFactory factory) {
    this.factory = factory;
  }

  public Reader<T> getReader(T.Builder builder) throws CrudException {
    CrudStore<T> store = factory.getCrudStore(builder);
    Reader<T> reader = new Reader<T>(factory, builder, this);
    readerList.add(reader);
    return reader;
  }

  public void close() throws CrudException {
    for (Reader reader : readerList) {
      reader.close();
    }
  }
}
