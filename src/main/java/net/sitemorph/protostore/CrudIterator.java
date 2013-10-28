package net.sitemorph.protostore;

/**
 * A generic crud iterator with the same operations as the underlying store
 * iterator.
 *
 * @author dak
 */
public interface CrudIterator<T>  {

  public T next() throws CrudException;

  public boolean hasNext() throws CrudException;

  public void close() throws CrudException;
}
