package net.sitemorph.protostore.sql;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudFactory;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.SortOrder;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A store that uses predefined naming conventions to bind proto to storage.
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
public class AutoNamedUrnStoreFactory implements CrudFactory {

  private static final String URN_FIELD = "urn";
  private static final String VECTOR = "vector";
  private static final String URN_SUFFIX = "Urn";

  private Set<String> indexFields;
  private Map<String, SortOrder> sortFields;

  private Connection connection;
  private Map<String, CrudStore<? extends Message>> stores = new HashMap<>();

  private AutoNamedUrnStoreFactory() {}

  public static Builder newBuilder(Connection connection) {
    return new Builder(connection);
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

    private Builder(Connection connection) {
      result = new AutoNamedUrnStoreFactory();
      result.connection = connection;
      result.indexFields = new HashSet<>();
      result.sortFields = new HashMap<>();
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
