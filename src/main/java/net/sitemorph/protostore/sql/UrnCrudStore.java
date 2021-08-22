package net.sitemorph.protostore.sql;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.MessageNotFoundException;
import net.sitemorph.protostore.MessageVectorException;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.protostore.ram.InMemoryStore;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static net.sitemorph.protostore.sql.AutoIdCrudStore.setStatementValue;

/**
 * URN keyed data store using columnar storage like the field iterator but uses
 * internal UUID. Also supports sort order.
 *
 * The goal of this class is to allow UUID based crud and avoid db locks with
 * multiple front end.
 *
 * @author damien@sitemorph.net
 *
 */
public class UrnCrudStore<T extends Message> implements CrudStore<T> {

  private Connection connection;
  private PreparedStatement create, readAll, update, delete, readUrn;
  private String tableName;
  private Message.Builder prototype;
  private FieldDescriptor urnField;
  private Map<FieldDescriptor, PreparedStatement> readIndexes;
  private SortOrder sortDirection;
  private FieldDescriptor sortField;
  private FieldDescriptor vectorField;

  private UrnCrudStore() {
    readIndexes = new HashMap<>();
  }

  /**
   * Create a urn based object with a defined urn field.
   *
   * @param builder to build from
   * @return the constructed object with urn set.
   * @throws CrudException upon storage error
   */
  @Override
  public T create(T.Builder builder) throws CrudException {
    try {
      // set the uuid
      UUID uid = UUID.randomUUID();
      CrudIterator<T> prior = read(prototype.clone()
          .setField(urnField, uid.toString()));
      while (prior.hasNext()) {
        prior.close();
        uid = UUID.randomUUID();
        prior = read(prototype.clone()
            .setField(urnField, uid.toString()));
      }
      prior.close();
      builder.setField(urnField, uid.toString());
      if (null != vectorField) {
        InMemoryStore.setInitialVector(builder, vectorField);
      }

      Descriptor descriptor = prototype.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      int offset = 1;
      for (FieldDescriptor field : fields) {
        setStatementValue(create, offset++, field,builder.getField(field));
      }
      create.executeUpdate();
      //noinspection unchecked
      return (T) builder.build();
    } catch (SQLException e) {
      throw new CrudException("Error creating new urn crud object", e);
    }
  }

  /**
   * Read from the store using either primary or secondary indexes if set up.
   * If no value is specified in either a primary or secondary index field all
   * records are returned. If multiple secondary index fields are set then it
   * it an implementation decision which to use to index the result.
   *
   * @param builder with either urn or secondary index set.
   * @return iterator over results.
   * @throws CrudException upon storage error reading
   */
  @Override
  public CrudIterator<T> read(Message.Builder builder) throws CrudException {
    try {
      if (builder.hasField(urnField)) {
        readUrn.setString(1, builder.getField(urnField).toString());
        return new DbFieldIterator<>(builder, readUrn.executeQuery());
      }

      for (Map.Entry<FieldDescriptor, PreparedStatement> index :
          readIndexes.entrySet()) {
        FieldDescriptor field = index.getKey();
        PreparedStatement statement = index.getValue();
        if (builder.hasField(field)) {
          Object value = builder.getField(field);
          setStatementValue(statement, 1, field, value);
          return new DbFieldIterator<>(builder, statement.executeQuery());
        }
      }

      return new DbFieldIterator<>(builder, readAll.executeQuery());
    } catch (SQLException e) {
      throw new CrudException("Error reading urn fields records.", e);
    }
  }

  @Override
  public T readOne(Message.Builder prototype) throws CrudException {
    CrudIterator<T> items = read(prototype);
    if (!items.hasNext()) {
      items.close();
      throw new MessageNotFoundException("Message not found: " + prototype.toString());
    }
    T result = items.next();
    items.close();
    return result;
  }


  @Override
  public T update(T.Builder builder) throws CrudException {
    if(!builder.hasField(urnField)) {
      throw new CrudException("Can't update message due to missing urn");
    }

    // write the update
    try {
      Descriptor descriptor = builder.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      int offset = 1;
      long vector = -1;
      for (FieldDescriptor field : fields) {
        if (field.equals(urnField)) {
          // skip the urn field as it is set in the where
          continue;
        }
        if (field.equals(vectorField)) {
          // update the vector
          vector = (Long) builder.getField(vectorField);
          InMemoryStore.updateVector(builder, vectorField);
        }
        Object value = builder.hasField(field)? builder.getField(field) : null;
        setStatementValue(update, offset++, field, value);
      }

      update.setString(offset++,  builder.getField(urnField).toString());

      if (null != vectorField) {
        update.setLong(offset, vector);
      }
      int updated = update.executeUpdate();
      // test and set using update where old value to new value
      if (1 != updated) {
        throw new MessageVectorException(
            builder.getDescriptorForType().getName() + " : " +
            builder.getField(urnField) + " not updated to to vector mismatch");

      }
      //noinspection unchecked
      return (T) builder.build();
    } catch (SQLException e) {
      throw new CrudException("Error updating urn crud value", e);
    }
  }

  @Override
  public void delete(T message) throws CrudException {
    if(!message.hasField(urnField)) {
      throw new CrudException("Can't update message due to missing urn");
    }
    try {
      delete.setString(1, message.getField(urnField).toString());
      if (null != vectorField) {
        Long vector = (Long)message.getField(vectorField);
        delete.setLong(2, vector);
      }
      int updated = delete.executeUpdate();
      if (1 != updated) {
        throw new MessageVectorException("Delete failed due to missing or " +
            "vector clock mismatch");
      }
    } catch (SQLException e) {
      throw new CrudException("Error deleting urn crud value: " +
          e.getMessage(), e);
    }
  }

  @Override
  public void close() throws CrudException {
    try {
      create.close();
      readAll.close();
      update.close();
      delete.close();
      for (Map.Entry<FieldDescriptor, PreparedStatement> index :
          readIndexes.entrySet()) {
        index.getValue().close();
      }
    } catch (SQLException e) {
      throw new CrudException("Error closing Db Urn Field Store", e);
    }
  }

  public static class Builder<F extends Message> {

    private UrnCrudStore<F> result;
    private Set<String> indexes = new HashSet<>();

    public Builder() {
      result = new UrnCrudStore<>();
    }

    public UrnCrudStore<F> build() throws CrudException {
      if (null == result.prototype) {
        throw new CrudException("Protobuf prototype required but not set.");
      }
      if (null == result.tableName) {
        throw new CrudException("Table name required but not set");
      }
      if (null == result.urnField) {
        throw new CrudException("Required urn field not set");
      }
      if (null == result.connection) {
        throw new CrudException("Connection null. Please provide a connector");
      }


      Descriptor descriptor = result.prototype.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      for (String index : indexes) {
        boolean found = false;
        for (FieldDescriptor field : fields) {
          if (field.getName().equals(index)) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new CrudException("An undefined index field was specified: " +
              index);
        }
      }

      // Create
      StringBuilder create = new StringBuilder();
      create.append("INSERT INTO ")
          .append(result.tableName)
          .append(" (");
      for (FieldDescriptor field : fields) {
        create.append(field.getName())
            .append(", ");
      }
      create.delete(create.length() - 2, create.length());
      create.append(") VALUES (");
      for (int i = 0; i < fields.size(); i++) {
        create.append("?, ");
      }
      create.delete(create.length() - 2, create.length());
      create.append(")");
      try {
        result.create = result.connection.prepareStatement(create.toString());
      } catch (SQLException e) {
        throw new CrudException("Error generating create of Urn Store", e);
      }

      // Read all
      try {
        result.readAll = AutoIdCrudStore.getStatement(result.connection,
            result.tableName, fields, null, result.sortField,
            result.sortDirection);
        // read indexes
        for (FieldDescriptor field : fields) {
          if (indexes.contains(field.getName())) {
            result.readIndexes.put(field,
                AutoIdCrudStore.getStatement(result.connection,
                    result.tableName, fields, field, result.sortField,
                    result.sortDirection));
          }
        }
        result.readUrn = AutoIdCrudStore.getStatement(result.connection,
            result.tableName, fields, result.urnField, result.sortField,
            result.sortDirection);
      } catch (SQLException e) {
        throw new CrudException("Error generating read of Urn Store", e);
      }

      // Update
      try {
        StringBuilder update = new StringBuilder();
        update.append("UPDATE ")
            .append(result.tableName)
            .append(" SET ");
        for (FieldDescriptor field : fields) {
          if (field.equals(result.urnField)) {
            continue;
          }
          update.append(field.getName())
              .append(" = ?, ");
        }
        update.delete(update.length() - 2, update.length());
        update.append(" WHERE ")
            .append(result.urnField.getName())
            .append(" = ?");
        if (null != result.vectorField) {
          update.append(" AND ")
              .append(result.vectorField.getName())
              .append(" = ?");
        }
        result.update = result.connection.prepareStatement(update.toString());
      } catch (SQLException e) {
        throw new CrudException("Error creating update for urn store", e);
      }

      // Delete
      try {
        StringBuilder delete = new StringBuilder();
        delete.append("DELETE FROM ")
            .append(result.tableName)
            .append(" WHERE ")
            .append(result.urnField.getName())
            .append(" = ?");
        if (null != result.vectorField) {
          delete.append(" AND ")
              .append(result.vectorField.getName())
              .append(" = ?");
        }
        result.delete = result.connection.prepareStatement(delete.toString());
      } catch (SQLException e) {
        throw new CrudException("Error creating delete for urn store", e);
      }

      return result;
    }

    public Builder<F> setConnection(Connection connection) {
      result.connection = connection;
      return this;
    }

    public Builder<F> setTableName(String tableName) {
      result.tableName = tableName;
      return this;
    }

    public Builder<F> setUrnColumn(String urnColumn) throws CrudException {
      return setUrnField(urnColumn);
    }

    public Builder<F> setUrnField(String urnField) throws CrudException {
      // look for the urn field descriptor in the field definition list
      Descriptor descriptor = result.prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(urnField)) {
          result.urnField = field;
          break;
        }
      }
      if (null == result.urnField) {
        StringBuilder fields = new StringBuilder();
        for (FieldDescriptor field : descriptor.getFields()) {
          fields.append(field.getName())
              .append(", ");
        }
        throw new CrudException("Error locating urn field by name " + urnField +
            " in field list " + fields.toString());
      }
      return this;
    }

    public Builder<F> setVectorField(String fieldName) throws CrudException {
      Descriptor descriptor = result.prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          result.vectorField = field;
          return this;
        }
      }
      throw new CrudException("Error locating vector field: " + fieldName);
    }

    public Builder<F> setPrototype(Message.Builder prototype) {
      result.prototype = prototype;
      return this;
    }

    public Builder<F> addIndexField(String indexField) {
      indexes.add(indexField);
      return this;
    }

    public Builder<F> setSortOrder(String fieldName, SortOrder direction)
        throws CrudException {
      result.sortDirection = direction;
      Descriptor descriptor = result.prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          result.sortField = field;
          break;
        }
      }
      if (null == result.sortField) {
        throw new CrudException("Error locating sort field name: " + fieldName);
      }
      return this;
    }
  }
}
