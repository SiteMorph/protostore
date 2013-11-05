package net.sitemorph.protostore;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of a store based on database column fields. This assumes that
 * the table uses an auto ID based keying system for generating IDs on insert.
 *
 * @author damien@sitemorph.net
 */
public class DbFieldCrudStore<T extends Message> implements CrudStore<T> {

  private static Logger log = LoggerFactory.getLogger(DbFieldCrudStore.class);
  private Connection connection;
  private PreparedStatement create;
  private PreparedStatement read;
  private PreparedStatement readAll;
  private PreparedStatement update;
  private PreparedStatement delete;
  private String tableName;
  private String autoIdColumn;
  private Message.Builder builderProtype;
  private FieldDescriptor idDescriptor;
  private Map<FieldDescriptor, PreparedStatement> readIndexes;

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


  @Override
  public T create(T.Builder builder) throws CrudException {
    try {
      Descriptor descriptor = builder.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
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
      builder.setField(idDescriptor, keys.getInt(1));
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
   * @param builder
   * @return iterator over accounts
   * @throws CrudException
   */
  @Override
  public CrudIterator<T> read(T.Builder builder) throws CrudException {

    try {

      if (builder.hasField(idDescriptor)) {
        Object value = builder.getField(idDescriptor);
        Integer idValue = (Integer) value;
        read.setInt(1, idValue);
        return new DbFieldIterator<T>(builder, read.executeQuery());
      }
      if (null == readIndexes) {
        return new DbFieldIterator<T>(builder, readAll.executeQuery());
      }

      for (Map.Entry<FieldDescriptor, PreparedStatement> entry :
          readIndexes.entrySet()) {
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
  public T update(T.Builder builder) throws CrudException {
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
      update.setInt(offset, (Integer)builder.getField(idDescriptor));
      update.executeUpdate();
      return (T) builder.build();
    } catch (SQLException e) {
      throw new CrudException("Error updating store", e);
    }
  }

  @Override
  public void delete(T message) throws CrudException {
    try {
      delete.setInt(1, (Integer)message.getField(idDescriptor));
      delete.executeUpdate();
    } catch (SQLException e) {
      throw new CrudException("Error deleting from store", e);
    }
  }

  private DbFieldCrudStore() {
  }

  public static class Builder<F extends Message> {

    private DbFieldCrudStore<F> result;
    private Set<String> indexes = Sets.newHashSet();

    public Builder() {
      result = new DbFieldCrudStore<F>();
    }

    public DbFieldCrudStore<F> build() throws CrudException {

      if (null == result.autoIdColumn) {
        throw new CrudException("Required field auto ID column missing");
      }
      Descriptor descriptor = result.builderProtype.getDescriptorForType();
      List<FieldDescriptor> fields = descriptor.getFields();
      for (FieldDescriptor field : fields) {
        if (field.getName().equals(result.autoIdColumn)) {
          result.idDescriptor = field;
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
      result.readIndexes = Maps.newHashMap();
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

    public Builder<F> setBuilderPrototype(F.Builder builderPrototype) {
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
      read.append(" ");;
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

  static void setStatementValue(PreparedStatement statement, int index,
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
      default:
        throw new CrudException("Index could not be generated for " +
            "unsupported type: " + field.getType().name());
    }
  }
}
