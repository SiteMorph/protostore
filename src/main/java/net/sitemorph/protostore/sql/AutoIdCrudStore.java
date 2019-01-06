package net.sitemorph.protostore.sql;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.ram.InMemoryStore;
import net.sitemorph.protostore.MessageNotFoundException;
import net.sitemorph.protostore.MessageVectorException;
import net.sitemorph.protostore.SortOrder;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Legacy database protobuf mapping storage engine which supports Auto ID
 * generation etc and typical database use cases.
 *
 * @author damien@sitemorph.net
 */
public class AutoIdCrudStore<T extends Message> implements CrudStore<T> {

  private Connection connection;
  private PreparedStatement create;
  private PreparedStatement read;
  private PreparedStatement readAll;
  private PreparedStatement update;
  private PreparedStatement delete;
  private String tableName;
  private String autoIdColumn;
  private T.Builder builderProtype;
  private FieldDescriptor idDescriptor;
  private ColumnType idType;
  private Map<FieldDescriptor, PreparedStatement> readIndexes;
  private FieldDescriptor vectorField;

  /**
   * Enum used for auto ID generation type casting.
   */
  private enum ColumnType {
    INTEGER,
    LONG
  }

  @Override
  public void close() throws CrudException {
    try {
      create.close();
      read.close();
      readAll.close();
      update.close();
      delete.close();
      for (Map.Entry<FieldDescriptor, PreparedStatement> index :
          readIndexes.entrySet()) {
        index.getValue().close();
      }
    } catch (SQLException e) {
      throw new CrudException("Error closing underly prepared statements", e);
    }
  }


  @SuppressWarnings("unchecked")
  @Override
  public T create(T.Builder builder) throws CrudException {
    try {
      Descriptor descriptor = builder.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      if (null != vectorField) {
        InMemoryStore.setInitialVector(builder, vectorField);
      }
      int offset = 1;
      for (FieldDescriptor field : fields) {
        if (field.equals(idDescriptor)) {
          continue;
        }
        if (!builder.hasField(field)) {
          create.setNull(offset, offset++);
          continue;
        }
        Object value = builder.getField(field);
        setStatementValue(create, offset++, field, value);
      }
      create.executeUpdate();
      ResultSet keys = create.getGeneratedKeys();
      keys.next();
      switch (idType) {
        case INTEGER: builder.setField(idDescriptor, keys.getInt(1));
          break;
        case LONG: builder.setField(idDescriptor, keys.getLong(1));
      }

      keys.close();
      return (T) builder.build();
    } catch (SQLException e) {
      throw new CrudException("Error inserting value", e);
    }
  }

  /**
   * List all accounts by specifying no values to the builder. Note that there
   * is no guarantee on which index match is made first in the case that more
   * than one index is defined. If you have more than one only one will be used
   * and it will be the first matched on the index hash map ordering.
   *
   * Also note that indexes require integral, enum or string values.
   *
   * @param builder context to read elements using a prototype.
   * @return iterator over accounts
   * @throws CrudException when there is an underlying storage error.
   */
  @Override
  public CrudIterator<T> readAll(T.Builder builder) throws CrudException {
    try {
      if (builder.hasField(idDescriptor)) {
        Object value = builder.getField(idDescriptor);
        setStatementValue(read, 1, idDescriptor, value);
        return new DbFieldIterator<T>(builder, read.executeQuery());
      }
      if (null == readIndexes) {
        return new DbFieldIterator<T>(builder, readAll.executeQuery());
      }

      for (Map.Entry<FieldDescriptor, PreparedStatement> entry : readIndexes.entrySet()) {
        FieldDescriptor field = entry.getKey();
        PreparedStatement statement = entry.getValue();
        if (builder.hasField(field)) {
          Object value = builder.getField(entry.getKey());
          setStatementValue(statement, 1, field, value);
          return new DbFieldIterator<T>(builder, statement.executeQuery());
        }
      }
      // no index value set so return all results
      return new DbFieldIterator<T>(builder, readAll.executeQuery());
    } catch (SQLException e) {
      throw new CrudException("Error reading value caused by SQL exception", e);
    }
  }

  @Override
  public T read(T.Builder prototype) throws CrudException {
    CrudIterator<T> items = readAll(prototype);
    if (!items.hasNext()) {
      items.close();
      throw new MessageNotFoundException("Message not found: " + prototype.toString());
    }
    T result = items.next();
    items.close();;
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T update(T.Builder builder) throws CrudException {
    if (!builder.hasField(idDescriptor)) {
      throw new CrudException("Can't update message due to missing ID");
    }
    if (null != vectorField) {
      if (!builder.hasField(vectorField)) {
        throw new MessageVectorException("Update is missing clock vector");
      }
      CrudIterator<T> priors = readAll(builder);
      if (!priors.hasNext()) {
        priors.close();
        throw new CrudException("Update attempted for unknown message: " +
            builder.getField(idDescriptor));
      }
      T prior = priors.next();
      priors.close();
      if (!prior.getField(vectorField).equals(builder.getField(vectorField))) {
        throw new MessageVectorException("Update vector is out of date");
      }
      InMemoryStore.updateVector(builder, vectorField);
    }
    try {
      Descriptor descriptor = builder.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      int offset = 1;
      for (FieldDescriptor field : fields) {
        if (field.equals(idDescriptor)) {
          continue;
        }
        if (!builder.hasField(field)) {
          update.setNull(offset, offset++);
          continue;
        }
        Object value = builder.getField(field);
        setStatementValue(update, offset++, field, value);
      }
      // update has the last field as the id 'always' as it is the where
      setStatementValue(update, offset, idDescriptor,
          builder.getField(idDescriptor));
      int updated = update.executeUpdate();
      if (0 == updated) {
        throw new MessageNotFoundException("Attempt to update failed as not found");
      }
      return (T) builder.build();
    } catch (SQLException e) {
      throw new CrudException("Error updating store", e);
    }
  }

  @Override
  public void delete(T message) throws CrudException {
    if(!message.hasField(idDescriptor)) {
      throw new CrudException("Can't delete message due to missing urn");
    }
    if (null != vectorField) {
      if (!message.hasField(vectorField)) {
        throw new MessageVectorException("Delete is missing clock vector");
      }
      CrudIterator<T> priors = readAll(message.toBuilder());
      if (!priors.hasNext()) {
        priors.close();
        throw new CrudException("Delete attempted for unknown message: " +
            message.getField(idDescriptor));
      }
      T prior = priors.next();
      priors.close();
      if (!prior.getField(vectorField).equals(message.getField(vectorField))) {
        throw new MessageVectorException("Delete vector is out of date");
      }
    }
    try {
      setStatementValue(delete, 1, idDescriptor,
          message.getField(idDescriptor));
      int deleted = delete.executeUpdate();
      if (0 == deleted) {
        throw new MessageNotFoundException("Attempt to delete failed as not found");
      }
    } catch (SQLException e) {
      throw new CrudException("Error deleting from store", e);
    }
  }

  private AutoIdCrudStore() {
  }

  public static class Builder<F extends Message> {

    private AutoIdCrudStore<F> result;
    private Set<String> indexes = new HashSet<>();

    public Builder() {
      result = new AutoIdCrudStore<F>();
    }

    public AutoIdCrudStore<F> build() throws CrudException {

      if (null == result.autoIdColumn) {
        throw new CrudException("Required field auto ID column missing");
      }
      Descriptor descriptor = result.builderProtype.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      for (FieldDescriptor field : fields) {
        if (field.getName().equals(result.autoIdColumn)) {
          result.idDescriptor = field;
          switch (field.getType()) {
            case INT64 :
            case UINT64 :
            case FIXED64 :
            case SFIXED64 :
            case SINT64 :
              result.idType = ColumnType.LONG;
              break;
            case INT32 :
            case FIXED32 :
            case UINT32 :
            case SFIXED32 :
            case SINT32 :
              result.idType = ColumnType.INTEGER;

            default: // DOUBLE, FLOAT,BOOL, STRING, GROUP, MESSAGE, BYTES, ENUM,
              // these are not auto increment types so this is an error state
          }
        }
      }
      if (null == result.idDescriptor) {
        throw new CrudException("Did not find index field descriptor");
      }

      // CREATE
      StringBuilder create = new StringBuilder();
      create.append("INSERT INTO ")
          .append(result.tableName)
          .append(" (");
      create.append(
          DbFieldIterator.getCrudFieldList(descriptor, result.idDescriptor));
      create.append(") VALUES (");
      // iterate over remaining fields adding parameters
      for(int i = 0; i < fields.size() - 1; i++) {
        create.append("?, ");
      }
      create.delete(create.length() - 2, create.length());
      create.append(")");
      try {
        result.create = result.connection.prepareStatement(
            create.toString(), Statement.RETURN_GENERATED_KEYS);
      } catch (SQLException e) {
        throw new CrudException("Error generating insert of account");
      }

      // READ
      result.readIndexes = new HashMap<>();
      try {
        // add extra indexes
        for (FieldDescriptor field : fields) {
          if (indexes.contains(field.getName())) {
            // TODO 20131002 Implement sort order support
            result.readIndexes.put(field, getStatement(result.connection,
                result.tableName, fields, field, null, null));
          }
        }
        result.read = getStatement(result.connection, result.tableName,
            fields, result.idDescriptor, null, null);
        result.readAll = getStatement(result.connection, result.tableName,
            fields, null, null, null);
      } catch (SQLException e) {
        throw new CrudException("Error building crud store", e);
      }

      // UPDATE
      StringBuilder update = new StringBuilder();
      update.append("UPDATE ")
          .append(result.tableName)
          .append(" SET ");
      for (FieldDescriptor field : fields) {
        if (field.equals(result.idDescriptor)) {
          continue;
        }
        update.append(field.getName())
            .append("= ?, ");
      }
      update.delete(update.length() - 2, update.length());
      update.append(" WHERE ")
          .append(result.idDescriptor.getName())
          .append(" = ?");
      try {
        result.update = result.connection.prepareStatement(update.toString());
      } catch (SQLException e) {
        throw new CrudException("Error creating crud update statement", e);
      }

      //DELETE
      StringBuilder delete = new StringBuilder();
      delete.append("DELETE FROM ")
          .append(result.tableName)
          .append(" WHERE ")
          .append(result.idDescriptor.getName())
          .append(" = ?");
      try {
        result.delete = result.connection.prepareStatement(delete.toString());
      } catch (SQLException e) {
        throw new CrudException("Error creating crud delete statement", e);
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

    public Builder<F> setAutoIdColumn(String autoIdColumn) {
      result.autoIdColumn = autoIdColumn;
      return this;
    }

    public Builder<F> setVectorField(String fieldName) {
      if (null == result.builderProtype) {
        throw new IllegalStateException("Can't set vector field as no " +
            "prototype has been set");
      }
      Descriptor descriptor = result.builderProtype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          result.vectorField = field;
          return this;
        }
      }
      throw new IllegalArgumentException("Can't find the requested vector " +
          "clock field: " + fieldName);
    }

    public Builder<F> setBuilderPrototype(Message.Builder builderPrototype) {
      result.builderProtype = builderPrototype;
      return this;
    }

    public Builder<F> addIndexField(String fieldName) {
      indexes.add(fieldName);
      return this;
    }
  }

  /**
   * Get a prepared statement selecting by a given field.
   *
   * @param fields list
   * @param match field or null if match all
   * @return prepared statement
   * @throws SQLException
   */
  public static PreparedStatement getStatement(Connection connection,
      String tableName, List<FieldDescriptor> fields, FieldDescriptor match,
      FieldDescriptor sortField, SortOrder order)
      throws SQLException {
    StringBuilder read = new StringBuilder();
    read.append("SELECT ");
    for (FieldDescriptor field : fields) {
      read.append(field.getName())
          .append(", ");
    }
    read.delete(read.length() - 2, read.length());
    read.append(" FROM ")
        .append(tableName);
    if (null != match) {
      read.append(" WHERE ")
          .append(match.getName())
          .append(" = ?");
    }
    if (null != sortField) {
      read.append(" ORDER BY ")
          .append(sortField.getName());
    }
    if (null != order) {
      read.append(" ");
      switch (order) {
        case ASCENDING : read.append("ASC");
          break;
        case DESCENDING: read.append("DESC");
      }
    }
    //log.debug("Created read statement {}", read.toString());
    return connection.prepareStatement(
        read.toString());
  }

  public static void setStatementValue(PreparedStatement statement, int index,
      FieldDescriptor field, Object value) throws SQLException, CrudException {
    if (null == value) {
      statement.setNull(index, index);
      return;
    }
    switch (field.getType()) {
      case INT64:
      case SINT64:
      case SFIXED64:
      case UINT64:
      case FIXED64:
        statement.setLong(index, (Long) value);
        break;
      case SINT32:
      case UINT32:
      case SFIXED32:
      case FIXED32:
      case INT32:
        statement.setInt(index, (Integer) value);
        break;
      case BOOL:
        statement.setBoolean(index, (Boolean) value);
        break;
      case STRING:
        statement.setString(index, (String) value);
        break;
      case ENUM:
        //statement.setString(index, ((Enum)value).name());
        statement.setString(index,
            ((Descriptors.EnumValueDescriptor)value).getName());
        break;
      case FLOAT:
        statement.setFloat(index, (Float)value);
        break;
      case DOUBLE:
        statement.setDouble(index, (Double)value);
        break;
      case BYTES :
        statement.setBytes(index, ((ByteString) value).toByteArray());
        break;
      default:
        throw new CrudException("Index could not be generated for " +
            "unsupported type: " + field.getType().name());
    }
  }
}
