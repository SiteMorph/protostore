package net.sitemorph.protostore;

import com.google.protobuf.Message;
import protobuf.codec.Codec;
import protobuf.codec.json.JsonCodec;

import java.io.IOException;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Iterator for Json blob stored objects.
 *
 * Note: this implementation assumes that only the data field is returned as a
 * string.
 *
 * @author dak
 */
public class DbJsonCrudIterator<T extends Message> implements CrudIterator<T> {

  private final ResultSet resultSet;
  private final T.Builder prototype;
  private final Codec codec = new JsonCodec();

  public DbJsonCrudIterator(T.Builder prototype, ResultSet resultSet) {
    this.prototype = prototype;
    this.resultSet = resultSet;
  }

  @Override
  public T next() throws CrudException {
    try {
      resultSet.next();
      String value = resultSet.getString(1);
      Message message = codec.toMessage(prototype.getDefaultInstanceForType()
          .getClass(), new StringReader(value));
      if (message.getDescriptorForType().getFullName().equals(
          prototype.getDescriptorForType().getFullName())) {
        return (T) message;
      }
      throw new CrudException("Wrong message type error. Expected " +
          prototype.getDescriptorForType().getFullName() + " but found " +
          message.getDescriptorForType().getFullName());
    } catch (SQLException e) {
      throw new CrudException("Error getting next object", e);
    } catch (IOException e) {
      throw new CrudException("IO Error reading message", e);
    }
  }

  @Override
  public boolean hasNext() throws CrudException {
    try {
      boolean hasNext = resultSet.next();
      resultSet.previous();
      return hasNext;
    } catch (SQLException e) {
      throw new CrudException("Error checking for next crud item", e);
    }
  }

  @Override
  public void close() throws CrudException {
    try {
      resultSet.close();
    } catch (SQLException e) {
      throw new CrudException("Error closing crud iterator.", e);
    }
  }
}
