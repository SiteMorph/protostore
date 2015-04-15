package net.sitemorph.protostore;

import java.sql.Connection;

/**
 * A SQL backed name based store factory
 */
public interface SqlNamedStoreFactory extends NamedStoreFactory {

  /**
   * Set the connection to use for the named store functionality.
   *
   * @param connection context
   */
  public void setConnection(Connection connection);
}
