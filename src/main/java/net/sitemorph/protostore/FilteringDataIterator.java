package net.sitemorph.protostore;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.List;

/**
 * A simple filtering iterator which looks for data index field value match.
 *
 * @author damien@sitemroph.net
 */
public class FilteringDataIterator<T extends Message>
    implements CrudIterator<T> {


  private final List<T> data;
  private final FieldDescriptor matchField;
  private final Object fieldValue;
  private volatile int index;
  private volatile boolean found;

  public FilteringDataIterator(List<T> data, FieldDescriptor matchField,
      Object fieldValue) {
    this.data = data;
    this.matchField = matchField;
    this.fieldValue = fieldValue;
    this.index = 0;
    this.found = false;
  }

  /**
   * Get the next item in the list. Having had next called, a further element
   * will have to be sook on the next iteration.
   * @return the next value or null if none found.
   * @throws CrudException
   */
  @Override
  public T next() throws CrudException {
    if (!hasNext()) {
      return null;
    }
    found = false;
    return data.get(index++);
  }

  @Override
  public boolean hasNext() throws CrudException {
    if (found && index < data.size()) {
      return true;
    }
    found = false;
    while (!found && index < data.size()) {
      if (data.get(index).getField(matchField).equals(fieldValue)) {
        found = true;
        break;
      }
      index++;
    }
    return found && index < data.size();
  }

  @Override
  public void close() throws CrudException {

  }
}
