package net.sitemorph.protostore;


import com.google.protobuf.Message;

import java.io.Closeable;

/**
 * A crud store supporting create read update and delete operations for a proto
 * message. This interface provides a low level storage interface for multiple
 * storage systems with features for:
 * - Primary indexes (unique resource name)
 * - Secondary indexes dependent on storage engine.
 * - Sorting dependent upon storage engine.
 * - Version clocks which rely on underlying locking mechanisms.
 *
 * @author dak
 */
public interface CrudStore<T extends Message> extends Closeable {

  /**
   * Create a representation from a builder using the underlying store.
   *
   * @param builder to construct a message from using the underlying store.
   * @return a constructed object
   */
  public T create(T.Builder builder) throws CrudException;

  /**
   * Read a representation based on the fields set in the prototype. The returned
   * messages match the prototype fields depending on the underlying store
   * selection support, e.g. setting a URN field would typically match the
   * identifier for that message and only one message would be returned. If the
   * underlying storage supports a secondary index then all messages matching
   * the supplied prototype secondary index value would be returned.
   *
   * @param prototype for the selection of messages
   * @return iterator over representation messages.
   */
  public CrudIterator<T> read(T.Builder prototype) throws CrudException;

  /**
   * Read a single message based on a prototype of the message. This method is a
   * special case of the above readAll but returns only one matching record or
   * throws an exception if none is found.
   *
   * @param prototype builder with matching fields set.
   * @return matching message
   * @throws MessageNotFoundException if no matching record is in the store.
   */
  public T readOne(T.Builder prototype) throws CrudException;

  /**
   * Update a representation given a builder update. This method relies on
   * internal message fields specific to the implementing class.
   *
   * @param builder with updates applied.
   * @return The updated representation.
   */
  public T update(T.Builder builder) throws CrudException;

  /**
   * Delete a representation. The identifier is dependent on the underlying
   * implementation.
   *
   * @param message to update
   */
  public void delete(T message) throws CrudException;


  /**
   * Closable extension which allows support for syntactic language 'sugar'
   *
   * @throws CrudException when there is an underlying storage exception.
   */
  @Override
  public void close() throws CrudException;
}
