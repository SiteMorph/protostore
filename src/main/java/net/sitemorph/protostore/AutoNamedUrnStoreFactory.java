package net.sitemorph.protostore;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;

/**
 * An automatically named Urn store factory that offers automatic named storage
 * for a collection of protobuf messages.
 *
 * It uses:
 *
 * - Message name for table name.
 * - urn field for urn mapping.
 * - fields ending Urn for index fields.
 * - fields named vector for version fields.
 *
 * @author damien@sitemorph.net
 */
public class AutoNamedUrnStoreFactory implements SqlNamedStoreFactory {

  private static final String URN_FIELD = "urn";
  private static final String VECTOR = "vector";
  private static final String URN_SUFFIX = "Urn";

  private Set<String> indexFields;
  private Map<String, SortOrder> sortFields;

  private Connection connection;
  private Map<String, CrudStore<? extends Message>> stores = Maps.newHashMap();

  private AutoNamedUrnStoreFactory() {}

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @Override
  public <T extends Message> boolean supported(T.Builder builder) {
    return stores.containsKey(builder.getDescriptorForType().getFullName());
  }

  @Override
  public <T extends Message> CrudStore<T> getCrudStore(T.Builder builder)
      throws CrudException {
    String name = builder.getDescriptorForType().getFullName();
    if (stores.containsKey(name) && null != stores.get(name)) {
      return (CrudStore<T>) stores.get(name);
    }
    DbUrnFieldStore.Builder<? extends Message> store =
        new DbUrnFieldStore.Builder<Message>();
    Descriptor descriptor = builder.getDescriptorForType();
    store.setConnection(connection)
        .setPrototype(builder)
        .setTableName(descriptor.getName())
        .setUrnField(URN_FIELD);
    for (FieldDescriptor field : descriptor.getFields()) {
      String fieldName = field.getName();
      if (VECTOR.equals(fieldName)) {
        store.setVectorField(VECTOR);
      }
      if (fieldName.endsWith(URN_SUFFIX) || indexFields.contains(fieldName)) {
        store.addIndexField(fieldName);
      }
      if (sortFields.containsKey(fieldName)) {
        store.setSortOrder(fieldName, sortFields.get(fieldName));
      }
    }
    CrudStore<? extends Message> result = store.build();
    stores.put(name, result);
    return (CrudStore<T>) result;
  }

  public static class Builder {

    private AutoNamedUrnStoreFactory result;

    private Builder() {
      result = new AutoNamedUrnStoreFactory();
      result.indexFields = Sets.newHashSet();
      result.sortFields = Maps.newHashMap();
    }

    public Builder registerSortField(String sortField, SortOrder order) {
      result.sortFields.put(sortField, order);
      return this;
    }

    public Builder registerIndexName(String fieldName) {
      result.indexFields.add(fieldName);
      return this;
    }

    public Builder registerMessage(Message.Builder builder) {
      result.stores.put(builder.getDescriptorForType().getFullName(), null);
      return this;
    }

    public AutoNamedUrnStoreFactory build() {
      return result;
    }
  }
}
