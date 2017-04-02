package net.sitemorph.protostore;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * Cached crud store loads the contents of a crud store into an in memory store and persists
 * writes to another crud store. The goal of this store is to fulfil use cases where reading
 * the complete data set repeatedly would involve numerous scans of the  full data set. This is
 * achieved using an in memory crud store to read all items and fulfil all read requests while
 * the underlying store handles all create, update, delete operations.
 */
public class CachedCrudStore<T extends Message> implements CrudStore<T>{

  @Override
  public T create(Builder builder) throws CrudException {
    return null;
  }

  @Override
  public CrudIterator<T> read(Builder builder) throws CrudException {
    return null;
  }

  @Override
  public T update(Builder builder) throws CrudException {
    return null;
  }

  @Override
  public void delete(T message) throws CrudException {

  }

  @Override
  public void close() throws CrudException {

  }
}
