package net.sitemorph.protostore;

import com.google.protobuf.Message;

/**
 * Named storage factory lookup functionality.
 *
 * @author damien@sitemorph.net
 */
public interface NamedStoreFactory {

  /**
   * Return true if a given type of protobuf message is supported by the store.
   *
   * @param builder to check for
   * @return true if the store supports it.
   */
  public boolean supported(Message.Builder builder);

  /**
   * Get a crud store for the parameterised message type.
   *
   * @param builder for message storage
   * @return the crud store
   * @throws CrudException
   */
  public CrudStore<? extends Message> getStore(Message.Builder builder)
      throws CrudException;
}
