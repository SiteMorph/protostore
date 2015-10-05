package net.sitemorph.protostore;

import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

import java.sql.Connection;
import java.util.Map;

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

  private SortOrder order = null;
  private String sortField = null;

  private Connection connection;
  private Map<String, CrudStore<? extends Message>> stores = Maps.newHashMap();

  public AutoNamedUrnStoreFactory() {}

  public AutoNamedUrnStoreFactory(String sortField, SortOrder order) {
    this.sortField = sortField;
    this.order = order;
  }

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @Override
  public boolean supported(Builder builder) {
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
    boolean sortFound = false;
    for (FieldDescriptor field : descriptor.getFields()) {
      if (VECTOR.equals(field.getName())) {
        store.setVectorField(VECTOR);
      }
      if (field.getName().endsWith(URN_SUFFIX)) {
        store.addIndexField(field.getName());
      }
      if (null != sortField && field.getName().equals(sortField)) {
        sortFound = true;
      }
    }
    if (sortFound && null != sortField && null != order) {
      store.setSortOrder(sortField, order);
    }
    CrudStore<? extends Message> result = store.build();
    stores.put(name, result);
    return (CrudStore<T>) result;
  }

  public AutoNamedUrnStoreFactory registerMessage(Message.Builder builder) {
    stores.put(builder.getDescriptorForType().getFullName(), null);
    return this;
  }
}
