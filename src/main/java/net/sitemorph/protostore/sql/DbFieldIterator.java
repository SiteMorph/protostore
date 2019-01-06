package net.sitemorph.protostore.sql;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Iterator that uses builder fields to index a prepared statement.
 *
 * Assumes that the prepared statement uses the field list from the builder
 *
 * @author dak
 *
 * TODO implement type converter interface for type mappers and allow
 * registration to allow arbitrary type mapping.
 */
public class DbFieldIterator<T extends Message> implements CrudIterator<T> {

  private final ResultSet resultSet;
  private Message.Builder prototype;

  public DbFieldIterator(Message.Builder builder, ResultSet resultSet) {
    // nasty setup required
    prototype = builder;
    this.resultSet = resultSet;
  }

  public static String getCrudFieldList(Descriptor descriptor, String alias,
      FieldDescriptor... skip) {
    StringBuilder result = new StringBuilder();
    List<FieldDescriptor> fields = descriptor.getFields();
    Set<FieldDescriptor> skipSet = new HashSet<>();
    for (int i = 0; null != skip && i < skip.length; i++) {
      skipSet.add(skip[i]);
    }
    for (FieldDescriptor field : fields) {
      if (skipSet.contains(field)) {
        continue;
      }
      if (null != alias) {
        result.append(alias)
            .append(".");
      }
      result.append(field.getName())
          .append(", ");
    }
    result.delete(result.length() - 2, result.length());
    return result.toString();
  }

  public static String getCrudFieldList(Descriptor descriptor,
      FieldDescriptor... skip) {
    return getCrudFieldList(descriptor, null, skip);
  }

  @SuppressWarnings("unchecked")
  @Override
  public T next() throws CrudException {
    Message.Builder next = prototype.clone();
    try {
      resultSet.next();
      Descriptor descriptor = prototype.getDescriptorForType();
      int offset = 1;
      for (FieldDescriptor field : descriptor.getFields()) {
        Object value;
        switch (field.getType()) {
          case DOUBLE :
            value = resultSet.getDouble(offset++);
            break;
          case FLOAT :
            value = resultSet.getFloat(offset++);
            break;
          case INT64:
          case SINT64:
          case SFIXED64:
          case UINT64:
          case FIXED64 :
            value = resultSet.getLong(offset++);
            break;
          case SINT32:
          case UINT32:
          case SFIXED32:
          case FIXED32:
          case INT32:
            value = resultSet.getInt(offset++);
            break;
          case BOOL:
            value = resultSet.getBoolean(offset++);
            break;
          case STRING:
            value = resultSet.getString(offset++);
            break;
          case ENUM :
            String key = resultSet.getString(offset++);
            EnumValueDescriptor enumDescriptor =
                field.getEnumType().findValueByName(key);
            if (null != key && null == enumDescriptor) {
              throw new CrudException("Error finding enum " +
                  field.getEnumType().getName() + " value " + key);
            }
            value = enumDescriptor;
            break;
          case BYTES :
            byte[] data = resultSet.getBytes(offset++);
            if (null != data && 0 < data.length) {
              value = ByteString.copyFrom(data);
            } else {
              value = null;
            }
            break;
          //case GROUP:
          //case MESSAGE:
          default:
            throw new CrudException("Unsupported proto field type: " +
                field.getType().name());
        }
        if (resultSet.wasNull() || null == value) {
          next.clearField(field);
        } else {
          next.setField(field, value);
        }
      }
      return (T) next.build();
    } catch (SQLException e) {
      throw new CrudException("Error reading proto field", e);
    }
  }

  @Override
  public boolean hasNext() throws CrudException {
    try {
      boolean forward = resultSet.next();
      resultSet.previous();
      return forward;
    } catch (SQLException e) {
      throw new CrudException("Error checking for next crud object", e);
    }
  }

  @Override
  public void close() throws CrudException {
    try {
      resultSet.close();
    } catch (SQLException e) {
      throw new CrudException("Error closing crud iterator", e);
    }
  }
}
