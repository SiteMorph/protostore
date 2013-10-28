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
}
