package net.sitemorph.protostore.helper;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.MessageNotFoundException;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Pre-loading cached crud store which loads all elements into memory on build.
 * Note that this is for small data stores as it reads all data. The store does
 * not automatically detect underlying storage changes caused by race
 * conditions.
 *
 * Note: that the preload urn store doesn't respect sort order.
 */
public class PreloadUrnCrudStore<T extends Message> implements CrudStore<T> {

  private CrudStore<T> writeStore;
  private Map<String, T> urnMap = new HashMap<>();
  private Set<FieldDescriptor> indexes = new HashSet<>();
  private FieldDescriptor urnDescriptor;

  private PreloadUrnCrudStore() {}

  public static class Builder<M extends Message> {

    private CrudStore<M> writeStore;
    private M.Builder prototype;
    private String urnField;
    private Set<String> indexes = new HashSet<>();

    public Builder() {}

    public Builder<M> setPrototype(M.Builder prototype) {
      this.prototype = prototype;
      return this;
    }

    public Builder<M> setUrnField(String urnField) {
      this.urnField = urnField;
      return this;
    }

    public Builder<M> addIndexField(String name) {
      indexes.add(name);
      return this;
    }

    public Builder<M> setWriteStore(CrudStore<M> writeStore) {
      this.writeStore = writeStore;
      return this;
    }

    public CrudStore<M> build() throws CrudException {
      PreloadUrnCrudStore<M> result = new PreloadUrnCrudStore<>();
      result.writeStore = writeStore;

      for (FieldDescriptor descriptor : prototype.getDescriptorForType().getFields()) {
        if (descriptor.getName().equals(urnField)) {
          result.urnDescriptor = descriptor;
        } else if (indexes.contains(descriptor.getName())) {
          result.indexes.add(descriptor);
        }
      }
      if (null == result.urnDescriptor) {
        throw new CrudException("Could not locate urn field: " + urnField);
      }

      CrudIterator<M> priors = writeStore.readAll(prototype);
      while (priors.hasNext()) {
        M prior = priors.next();
        String urn = String.valueOf(prior.getField(result.urnDescriptor));
        result.urnMap.put(urn, prior);
      }
      priors.close();
      return result;
    }
  }

  @Override
  public T create(T.Builder builder) throws CrudException {
    T result = writeStore.create(builder);
    String urn = String.valueOf(result.getField(urnDescriptor));
    urnMap.put(urn, result);
    return result;
  }

  @Override
  public CrudIterator<T> readAll(T.Builder builder) throws CrudException {
    // urn first
    if (builder.hasField(urnDescriptor)) {
      String urn = String.valueOf(builder.getField(urnDescriptor));
      if (!urnMap.containsKey(urn)) {
        throw new MessageNotFoundException("Could not find urn: " + urn);
      }
      List<T> singleton = new ArrayList<>();
      singleton.add(urnMap.get(urn));
      return new CollectionIterator<>(singleton);
    }

    // iterate over the index fields, if one set return that
    for (FieldDescriptor index : indexes) {
      if (builder.hasField(index)) {
        List<T> result = new ArrayList<>();
        for (Entry<String, T> entry : urnMap.entrySet()) {
          if (builder.getField(index).equals(entry.getValue().getField(index))) {
            result.add(entry.getValue());
          }
        }
        return new CollectionIterator<>(result);
      }
    }

    // return all data
    List<T> result = new ArrayList<>(urnMap.values());
    return new CollectionIterator<>(result);
  }

  @Override
  public T read(Message.Builder prototype) throws CrudException {
    CrudIterator<T> items = readAll(prototype);
    if (!items.hasNext()) {
      items.close();
      throw new MessageNotFoundException("Resource not found for: " + prototype.toString());
    }
    T result = items.next();
    items.close();
    return result;
  }

  @Override
  public T update(Message.Builder builder) throws CrudException {
    T updated = writeStore.update(builder);
    String urn = String.valueOf(updated.getField(urnDescriptor));
    urnMap.put(urn, updated);
    return updated;
  }

  @Override
  public void delete(T message) throws CrudException {
    String urn = String.valueOf(message.getField(urnDescriptor));
    urnMap.remove(urn);
    writeStore.delete(message);
  }

  @Override
  public void close() throws IOException {
    urnMap.clear();
    urnMap = null;
    writeStore.close();
  }
}
