package net.sitemorph.protostore;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A really simple in memory store that has some particular semantics.
 * This store is simply intended to be used as an in memory cache or
 * buffer rather than a persisted store. This means that you must set all
 * required fields for the builder before it is passed.
 *
 * As with the other database backed storage the in memory store does support
 * read with fields set.
 *
 * The store supports filtering by urn or index field. Note that these
 * operations are O(N) as they iterate over the full data set. The store
 * supports a single order by field as well as named secondary indexes. Results
 * are stored in the sort order specified so will
 *
 * @author damien@sitemorph.net
 */
public class InMemoryStore<T extends Message> implements CrudStore<T> {

  private FieldDescriptor urnField;
  private List<FieldDescriptor> indexes = Lists.newArrayList();
  private List<T> data = Lists.newArrayList();
  private Descriptor descriptor;
  private FieldDescriptor sortField = null;
  private SortOrder direction = SortOrder.ASCENDING;

  private InMemoryStore() {}

  @Override
  public T create(T.Builder builder) throws CrudException {
    T newValue = (T) builder.build();
    int insertAt;
    if (null != sortField) {
      insertAt = Collections.binarySearch(data, newValue, new Comparator<T>() {
        @Override
        public int compare(T left, T right) {
          Object leftValue = left.getField(sortField);
          if (!(leftValue instanceof Comparable)) {
            throw new IllegalArgumentException("Underlying type is not " +
                "comparable. Please check your confifuration. for sort field " +
                sortField.getName());
          }
          Object rightValue = right.getField(sortField);
          if (!(rightValue instanceof Comparable)) {
            throw new IllegalArgumentException("Underlying type is not " +
                "comparable. please check your configuration for sort field " +
                sortField.getName());
          }
          if (SortOrder.ASCENDING == direction) {
            return ((Comparable) leftValue).compareTo(rightValue);
          } else {
            return ((Comparable) rightValue).compareTo(leftValue);
          }
        }
      });
      if (0 > insertAt) {
        // if not exactly found then will be inserted.
        insertAt = -(insertAt) - 1;
      }
      // otherwise will be inserted at the position of the first matching
      // value according to the insert order.
    } else {
      insertAt = data.size();
    }
    // TODO 20131111 Consider a scan to remove stale objects based on urn
    data.add(insertAt, newValue);
    return newValue;
  }

  @Override
  public CrudIterator<T> read(T.Builder builder) throws CrudException {
    if (builder.hasField(urnField)) {
      // read based on the urn field
      return new FilteringDataIterator<T>(data, urnField,
          builder.getField(urnField));
    }
    // iterate over the index fields
    for (FieldDescriptor field : indexes) {
      if (builder.hasField(field)) {
        return new FilteringDataIterator(data, field,
            builder.getField(field));
      }
    }
    // read all data
    return new AllDataIterator<T>(data);
  }

  @Override
  public T update(T.Builder builder) throws CrudException {
    if (!builder.hasField(urnField)) {
      throw new IllegalArgumentException("Update provided does not include " +
          "a value for the urn field");
    }
    Object updateUrn = builder.getField(urnField);
    for (int i = 0; i < data.size(); i++) {
      T old = data.get(i);
      if (old.getField(urnField).equals(updateUrn)) {
        T result = (T) builder.build();
        data.set(i, result);
        return result;
      }
    }
    throw new IllegalArgumentException("Update passed message that was not " +
        "stored. Update not possible");
  }

  @Override
  public void delete(T message) throws CrudException {
    // TODO 20131111 Implement based on urn column
    data.remove(message);
  }

  @Override
  public void close() throws CrudException {
    data.clear();
    indexes.clear();

  }

  public static class  Builder<M extends Message> {

    private InMemoryStore<M> result;
    private M.Builder prototype;

    public Builder() {
      result = new InMemoryStore<M>();
    }

    public Builder setPrototype(M.Builder prototype) {
      this.prototype = prototype;
      result.descriptor = prototype.getDescriptorForType();
      return this;
    }

    /**
     * Set the urn field for the store based on the name of the field.
     * @param fieldName to find.
     * @return builder
     * @throws IllegalStateException if you have not set the prototype first
     * @throws IllegalArgumentException if you pass a name not found.
     */
    public Builder setUrnField(String fieldName) {
      if (null == prototype) {
        throw new IllegalStateException("Can't choose field based on name " +
            "because no prototype has been set");
      }
      Descriptor descriptor = prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          result.urnField = field;
          return this;
        }
      }
      throw new IllegalArgumentException("Supplied field name " + fieldName +
          " did not match any descriptor field names");
    }

    public Builder addIndexField(String fieldName) {
      if (null == prototype) {
        throw new IllegalStateException("Can't add index field as no " +
            "prototype has been set");
      }
      Descriptor descriptor = prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          result.indexes.add(field);
          return this;
        }
      }
      throw new IllegalArgumentException("Supplied field name " + fieldName +
          "did not match any field descriptor field names");
    }

    public Builder setSortOrder(String fieldName, SortOrder direction) {
      if (null == prototype) {
        throw new IllegalStateException("Can't set sort order field as no " +
            "prototype has been set");
      }
      Descriptor descriptor = prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          switch (field.getType()) {
            case BYTES:
            case ENUM:
            case GROUP:
            case MESSAGE:
              throw new IllegalArgumentException("Field " + fieldName +
                  " has complex type " + field.getType().name() + " which " +
                  "can't be used as a sort field");
          }
          result.sortField = field;
          result.direction = direction;
          return this;
        }
      }
      throw new IllegalArgumentException("Supplied field name " + fieldName +
          "did not match any field descriptor field names.");
    }

    public InMemoryStore<M> build() {
      return result;
    }
  }
}
