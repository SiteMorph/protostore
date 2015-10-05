package net.sitemorph.protostore;

import com.google.protobuf.Message;

/**
 * Named storage factory lookup functionality.
 *
 * @author damien@sitemorph.net
 */
public interface NamedStoreFactory extends CrudFactory {

  /**
   * Return true if a given type of protobuf message is supported by the store.
   *
   * @param builder to check for
   * @return true if the store supports it.
   */
  public boolean supported(Message.Builder builder);
}
