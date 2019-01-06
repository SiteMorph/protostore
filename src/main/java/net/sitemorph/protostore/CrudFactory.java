package net.sitemorph.protostore;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;

/**
 * Factory marker interface allows a class to indicate that it supports
 * the generation of different proto based stores.
 *
 * @author damien@sitemorph.net
 */
public interface CrudFactory {

  public <T extends MessageOrBuilder> CrudStore<T> getCrudStore(T builder)
      throws CrudException;
}
