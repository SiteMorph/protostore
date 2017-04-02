package net.sitemorph.protostore;

import com.google.protobuf.Message;

/**
 * Cached crud store loads the contents of a crud store into an in memory store and persists
 * writes to another crud store. The goal of this store is to fulfil use cases where reading
 * the complete data set repeatedly would involve numerous scans of the  full data set. This is
 * achieved using an in memory crud store to read all items and fulfil all read requests while
 * the underlying store handles all create, update, delete operations.
 */
public class CachedCrudStore<T extends Message> implements CrudStore<T> {

  private final CrudStore<T> writeStore;
  private final InMemoryStore<T> cache;

  private CachedCrudStore(CrudStore<T> writeStore, InMemoryStore<T> cache) {
    this.writeStore = writeStore;
    this.cache = cache;
  }

  public static class Builder<M extends Message> {

    private InMemoryStore.Builder<M> builder;
    private CrudStore<M> writeStore;
    private M.Builder prototype;

    public Builder() {
      builder = new InMemoryStore.Builder<M>();
    }

    public Builder<M> setPrototype(M.Builder prototype) {
      builder.setPrototype(prototype);
      this.prototype = prototype;
      return this;
    }

    public Builder<M> setUrnField(String urnField) {
      builder.setUrnField(urnField);
      return this;
    }

    public Builder<M> addIndexField(String name) {
      builder.addIndexField(name);
      return this;
    }

    public Builder<M> setSortOrder(String fieldname, SortOrder order) {
      builder.setSortOrder(fieldname, order);
      return this;
    }

    public Builder<M> setVectorField(String fieldName) {
      builder.setVectorField(fieldName);
      return this;
    }

    public Builder<M> setWriteStore(CrudStore<M> writeStore) {
      this.writeStore = writeStore;
      return this;
    }

    public CrudStore<M> build() throws CrudException {
      InMemoryStore<M> memory = (InMemoryStore<M>) builder.build();
      CrudIterator<M> priors = writeStore.read(prototype);
      while (priors.hasNext()) {
        memory.add(priors.next());
      }
      priors.close();;
      return new CachedCrudStore<M>(writeStore, memory);
    }
  }

  @Override
  public T create(Message.Builder builder) throws CrudException {
    return null;
  }

  @Override
  public CrudIterator<T> read(Message.Builder builder) throws CrudException {
    return cache.read(builder);
  }

  @Override
  public T update(Message.Builder builder) throws CrudException {
    T updated = writeStore.update(builder);
    cache.refresh(updated);
    return updated;
  }

  @Override
  public void delete(T message) throws CrudException {

  }

  @Override
  public void close() throws CrudException {

  }
}
