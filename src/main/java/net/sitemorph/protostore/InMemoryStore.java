package net.sitemorph.protostore;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

/**
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
 * are stored in the sort order specified so will be returned in that order
 * whenever iterated over.
 *
 * Note that it is assumed that the urn field is a UUID string and will be set
 * as such when create is called, checking for uniqueness.
 *
 * If set up when creating the store support for vector clocks is also provided.
 * This is achieved by the use of 'reads' having a hinted vector clock field
 * which must store a signed long value which is used to signal updates to the
 * store. Vector values should not be set but will be required on update. This
 * means that a read / update can be checked to ensure that writes to a message
 * store are reflective of updates made. If two readers read concurrently one
 * will succeed in the write then the other, the second will throw an update
 * vector error due to clock skew.
 *
 * @author damien@sitemorph.net
 */
public class InMemoryStore<T extends Message> implements CrudStore<T> {

  public  static final long INITIAL_VECTOR = 0;
  private FieldDescriptor urnField;
  private List<FieldDescriptor> indexes = Lists.newArrayList();
  private List<T> data = Lists.newArrayList();
  private Descriptor descriptor;
  private FieldDescriptor sortField = null;
  private SortOrder direction = SortOrder.ASCENDING;
  private FieldDescriptor vectorField = null;

  private InMemoryStore() {}

  @Override
  public synchronized T create(Message.Builder builder) throws CrudException {

    // find a urn for the new object
    UUID urn = UUID.randomUUID();

    CrudIterator<T> priors = new FilteringDataIterator<T>(data, urnField,
        urn);
    while (priors.hasNext()) {
      urn = UUID.randomUUID();
      priors = new FilteringDataIterator<T>(data, urnField, urn);
    }
    builder.setField(urnField, urn.toString());
    if (null != vectorField) {
      setInitialVector(builder, vectorField);
    }

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
  public synchronized  CrudIterator<T> read(Message.Builder builder) throws CrudException {
    if (builder.hasField(urnField)) {
      // read based on the urn field
      return new FilteringDataIterator<T>(Lists.newArrayList(data), urnField,
          builder.getField(urnField));
    }
    // iterate over the index fields
    for (FieldDescriptor field : indexes) {
      if (builder.hasField(field)) {
        return new FilteringDataIterator<T>(Lists.newArrayList(data), field,
            builder.getField(field));
      }
    }
    // read all data
    return new AllDataIterator<T>(Lists.newArrayList(data));
  }

  @Override
  public synchronized T update(Message.Builder builder) throws CrudException {
    if (!builder.hasField(urnField)) {
      throw new IllegalArgumentException("Update provided does not include " +
          "a value for the urn field");
    }
    Object updateUrn = builder.getField(urnField);
    for (int i = 0; i < data.size(); i++) {
      T old = data.get(i);
      // if this is our message to update
      if (old.getField(urnField).equals(updateUrn)) {
        if (null != vectorField) {
          if (!builder.hasField(vectorField)) {
            throw new MessageVectorException("Update is missing clock vector");
          }
          if (!builder.getField(vectorField).equals(old.getField(vectorField))) {
            throw new MessageVectorException("Update vector is out of date");
          }
          updateVector(builder, vectorField);
        }

        T result = (T) builder.build();
        data.set(i, result);
        // sort the data in case the update order changed
        Collections.sort(data, new Comparator<T>() {
          @Override
          public int compare(T left, T right) {
            Object leftValue = left.getField(sortField);
            if (!(leftValue instanceof Comparable)) {
              throw new IllegalArgumentException("Underlying type is not " +
                  "comparable. Please check your configuration. for sort field " +
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
        return result;
      }
    }
    throw new MessageNotFoundException("Update passed message that was not " +
        "stored. Update not possible");
  }

  @Override
  public synchronized void delete(T message) throws CrudException {
    // TODO 20131111 Implement based on urn column
    for (int i = 0; i < data.size(); i++) {
      T old = data.get(i);
      if (old.getField(urnField).equals(message.getField(urnField))) {
        if (null != vectorField) {
          if (!message.getField(vectorField).equals(old.getField(vectorField))) {
            throw new MessageVectorException("Update failed due to vector " +
                "mismatch");
          }
        }
        data.remove(i);
        return;
      }
    }
    throw new MessageNotFoundException("Failed to delete missing message");
  }

  @Override
  public void close() throws CrudException {
    data.clear();
    indexes.clear();

  }

  static void updateVector(Message.Builder builder,
      FieldDescriptor vectorField) {
    Object current = builder.getField(vectorField);
    if (null == current) {
      throw new RuntimeException("Message clock vector data missing");
    }
    if (!(current instanceof Long)) {
      throw new RuntimeException("Message clock vector data type error");
    }
    Long value = (Long)current;
    if (value == Long.MAX_VALUE) {
      value = INITIAL_VECTOR;
    } else {
      value += 1L;
    }
    builder.setField(vectorField, value);
  }

  static void setInitialVector(Message.Builder builder,
      FieldDescriptor vectorField) {
    builder.setField(vectorField, INITIAL_VECTOR);
  }

  public static class  Builder<M extends Message> {

    private InMemoryStore<M> result;
    private Message.Builder prototype;
    private static final EnumSet<Type> INTEGRALS = EnumSet.of(
        Type.INT64,
        Type.UINT64,
        Type.FIXED64,
        Type.SFIXED64,
        Type.SINT64);

    public Builder() {
      result = new InMemoryStore<M>();
    }

    public Builder<M> setPrototype(Message.Builder prototype) {
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
    public Builder<M> setUrnField(String fieldName) {
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

    public Builder<M> addIndexField(String fieldName) {
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

    public Builder<M> setVectorField(String fieldName) {
      if (null == prototype) {
        throw new IllegalStateException("Can't set vector field as no " +
            "prototype has been set");
      }
      Descriptor descriptor = prototype.getDescriptorForType();
      for (FieldDescriptor field : descriptor.getFields()) {
        if (field.getName().equals(fieldName)) {
          if (!INTEGRALS.contains(field.getType())) {
            throw new IllegalArgumentException("Can't use a vector clock " +
                "on a non long field");
          }
          result.vectorField = field;
          return this;
        }
      }
      throw new IllegalArgumentException("Can't find the requested vector " +
          "clock field: " + fieldName);
    }

    public Builder<M> setSortOrder(String fieldName, SortOrder direction) {
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

    public CrudStore<M> build() {
      if (null == result.sortField) {
        result.sortField = result.urnField;
      }
      return result;
    }
  }
}
