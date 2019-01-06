package net.sitemorph.protostore;


import com.google.protobuf.GeneratedMessageV3;

/**
 * A basic store supporting create read update and delete operations for a proto
 * message.
 *
 * @author dak
 */
public interface CrudStore<T extends GeneratedMessageV3> {

  /**
   * Create a representation from a builder.
   * @param builder to build from
   * @return a constructed object
   */
  public T create(T builder) throws CrudException;

  /**
   * Read a representation based on the fields set in the builder. This method
   * supports both ID and secondary index selection.
   *
   * @param builder for the selection of messages
   * @return iterator over representation messages.
   */
  public CrudIterator<T> read(T builder) throws CrudException;

  /**
   * Update a representation given a builder update. This method relies on
   * internal message fields specific to the implementing class.
   *
   * @param builder with updates applied.
   * @return The updated representation.
   */
  public T update(T builder) throws CrudException;

  /**
   * Delete a representation. The identifier is dependent on the underlying
   * implementation.
   *
   * @param message to update
   */
  public void delete(T message) throws CrudException;

  public void close() throws CrudException;
}
