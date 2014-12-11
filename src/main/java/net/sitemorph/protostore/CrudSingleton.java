package net.sitemorph.protostore;

/**
 * Get a singleton from a crud reader.
 *
 * @author damien@sitemorph.net
 */

import com.google.protobuf.Message;

public class CrudSingleton {

  public static <T extends Message> T read(T.Builder builder,
      CrudFactory factory) throws CrudException {
    CrudStore<T> store = factory.getCrudStore(builder);
    CrudIterator<T> iterator = store.read(builder);
    if (!iterator.hasNext()) {
      iterator.close();
      throw new MessageNotFoundException("Could not locate singleton");
    }
    T result = iterator.next();
    iterator.close();
    return result;
  }
}
