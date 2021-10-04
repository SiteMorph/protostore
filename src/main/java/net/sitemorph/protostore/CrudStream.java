package net.sitemorph.protostore;

import com.google.protobuf.Message;

import java.util.stream.Stream;

/**
 * Marker interface for crud streaming.
 *
 * Allows streaming of a type either utilising an underlying storage level read filter or a full bucket.
 */
public interface CrudStream<T extends Message> {

  boolean supportsStreams();

  Stream<T> stream(T.Builder builder);

}
