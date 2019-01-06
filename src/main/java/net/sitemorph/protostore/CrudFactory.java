package net.sitemorph.protostore;

import com.google.protobuf.Message;

/**
 * Factory marker interface allows a class to indicate that it supports
 * the generation of different proto based stores.
 *
 * @author damien@sitemorph.net
 */
public interface CrudFactory {

  public <T extends Message> CrudStore<T> getCrudStore(T.Builder builder)
      throws CrudException;

  /**
   * Return true if a given type of protobuf message is supported by the store.
   *
   * @param builder to check for
   * @return true if the store supports it.
   */
  public <T extends Message> boolean supported(T.Builder builder);
}
