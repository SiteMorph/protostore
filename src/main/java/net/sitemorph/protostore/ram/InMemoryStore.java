package net.sitemorph.protostore.ram;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message;
import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.MessageNotFoundException;
import net.sitemorph.protostore.MessageVectorException;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.protostore.helper.CollectionIterator;
import net.sitemorph.protostore.helper.FilteringDataIterator;
import net.sitemorph.protostore.helper.IteratorAdaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * In memory reference implementation of the Protostore CRUD interface with
 * support for:
 * * UUID class 4 random unique resource identifier allocation on create
 * * Multiple index field iteration
 * * Sort order traversal
 * * Vector clock check then set locking semantics for message updates
 *
 * The in memory (RAM) store uses a sorted storage list to achieve balanced
 * performance with operations currently around:
 *
 * Create: T(N + log(N))
 * Read: T(N)
 * Update: T(N/2)
 * Delete T(N/2)
 *
 * @author hello@damienallison.com
 */
public class InMemoryStore<T extends Message> implements CrudStore<T> {

  private static final long INITIAL_VECTOR = 0;
  private FieldDescriptor urnField;
  private final List<FieldDescriptor> indexes = new ArrayList<>();
  private final List<T> data = new ArrayList<>();
  private FieldDescriptor sortField = null;
  private SortOrder direction = SortOrder.ASCENDING;
  private FieldDescriptor vectorField = null;

  private InMemoryStore() {}

  @Override
  public synchronized T create(T.Builder builder) throws CrudException {

    // find a urn for the new object
    UUID urn = UUID.randomUUID();

    CrudIterator<T> priors = new FilteringDataIterator<>(data, urnField,
        urn);
    while (priors.hasNext()) {
      urn = UUID.randomUUID();
      priors = new FilteringDataIterator<>(data, urnField, urn);
    }
    builder.setField(urnField, urn.toString());
    if (null != vectorField) {
      setInitialVector(builder, vectorField);
    }
    @SuppressWarnings("unchecked")
    T newValue = (T) builder.build();
    int insertAt;
    if (null != sortField) {
      insertAt = Collections.binarySearch(data, newValue, new InMemoryComparator<>(sortField, direction));

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
  public synchronized  CrudIterator<T> read(Message.Builder builder) {
    if (builder.hasField(urnField)) {
      // read based on the urn field
      return new FilteringDataIterator<>(new ArrayList<>(data), urnField,
          builder.getField(urnField));
    }
    // iterate over the index fields
    for (FieldDescriptor field : indexes) {
      if (builder.hasField(field)) {
        return new FilteringDataIterator<>(new ArrayList<>(data), field,
            builder.getField(field));
      }
    }
    // read all data
    return new CollectionIterator<>(new ArrayList<>(data));
  }

  @Override
  public T readOne(T.Builder prototype) throws CrudException {
    CrudIterator<T> items = read(prototype);
    if (!items.hasNext()) {
      items.close();
      throw new MessageNotFoundException("Message not found: " + prototype);
    }
    T result = items.next();
    items.close();
    return result;
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
        //noinspection unchecked
        T result = (T) builder.build();
        data.set(i, result);
        // sort the data in case the update order changed
        data.sort(new InMemoryComparator<>(sortField, direction));
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
  public void close() {
  }

  public static void updateVector(Message.Builder builder,
      FieldDescriptor vectorField) {
    Object current = builder.getField(vectorField);
    if (null == current) {
      throw new RuntimeException("Message clock vector data missing");
    }
    if (!(current instanceof Long)) {
      throw new RuntimeException("Message clock vector data type error");
    }
    long value = (Long)current;
    if (value == Long.MAX_VALUE) {
      value = INITIAL_VECTOR;
    } else {
      value += 1L;
    }
    builder.setField(vectorField, value);
  }

  public static void setInitialVector(Message.Builder builder,
      FieldDescriptor vectorField) {
    builder.setField(vectorField, INITIAL_VECTOR);
  }

  @Override
  public Stream<T> stream(T.Builder builder) {
    IteratorAdaptor<T> adapter = new IteratorAdaptor<>(this.read(builder));
    int characteristics = Spliterator.CONCURRENT | Spliterator.NONNULL;
    if (null != sortField) {
      characteristics |= Spliterator.ORDERED;
    }
    Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(adapter, characteristics);
    return StreamSupport.stream(spliterator, false);
  }

  @Override
  public boolean supportsStreams() {
    return true;
  }

  public static class  Builder<M extends Message> {

    private final InMemoryStore<M> result;
    private Message.Builder prototype;
    private static final EnumSet<Type> INTEGRALS = EnumSet.of(
        Type.INT64,
        Type.UINT64,
        Type.FIXED64,
        Type.SFIXED64,
        Type.SINT64);

    public Builder() {
      result = new InMemoryStore<>();
    }

    public Builder<M> setPrototype(Message.Builder prototype) {
      this.prototype = prototype;
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

  private static class InMemoryComparator<L extends Message> implements Comparator<L> {

    private final FieldDescriptor sortField;
    private final SortOrder direction;

    InMemoryComparator(FieldDescriptor sortField, SortOrder direction) {
      this.sortField = sortField;
      this.direction = direction;
    }

      @Override
      public int compare(L left, L right) {
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
          //noinspection rawtypes,
          return ((Comparable) leftValue).compareTo(rightValue);
        } else {
          //noinspection rawtypes
          return ((Comparable) rightValue).compareTo(leftValue);
        }
      }
  }
}
