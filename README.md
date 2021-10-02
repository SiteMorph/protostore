protostore - A very simple CRUD storage system for protobuf messages
====================================================================

Google Protobuf Serialisation Data Access Object Library.

Supports serialisation of protobuf messages to sql columnar storage with
Create Read Update Delete semantics, primary key on UUID or auto-increment and
secondary indexes.

The goal of this project is to provide a very light weight mechanism to support
CRUD operations of any proto message, primarily to avoid writing SQL.

In java the library has already helped many times with database and object
refactorings. For example to rename a column in your database:

1. Generate your proto java.
2. Using your IDE rename get<Field> and has<Field> in the message, set<Field>
    and clear<Field> resulting in all code migrating to the new format.
3. Modify you proto field name.
4. Re-generate your proto.
5. Refactor your database.
6. Deploy your updated proto access.
7. Clean up your database rubbish columns.
8. Joy. No additional tests. No additional SQL.

Many database and object refactorings follow a similar pattern.

Features
--------
* Store protobuf to SQL / memory.
* Write proto messages for legacy database tables without having to write sql.
* Primary index support with auto increment and java UUID key support.
* Secondary indexes on additional field values.
* Ordered data by field value.
* Message clock vector support for create / update / delete to detect race
  conditions.

Limitations
-----------
* MySQL like syntax for now.
* Only supports iteration of values, no aggregation functions.
* No roll back on message build error.
* Specific drivers weren't adapted to use a vector adaptor to allow different
  drivers to use internal features for locking as there is still no general
  distributed locking solution assumed.
* The clock vector solution doesn't include create version as would be included
  in protocols like chubby to avoid accidental allocation of locks as it is
  currently assumed that the allocation of UUID to a random value will be
  sufficient to avoid duplication of implied lock ownership.

Usage
-----

Assuming that you are simply using the proto storage to a legacy SQL database
you have two options:

* DbFieldCrudStore: Used for auto incremented keyed tables.
* DbUrnFieldStore: Uses Java UUID for key value which is better for large web apps
    due to avoidance of lock / centralised auto-increment.

Both store examples follow a similar usage pattern:

```java

  CrudStore<MyMessage> store = new DbUrnFieldStore.Builder<MyMessage>()
      .setConnection(sqlConnection)
      .setPrototype(MyMessage.newBuilder())
      .setTableName("MyMessageTable")
      .setUrnField("urn")
      .setVectorField("vector")
      .addIndexField("secondaryIndex")
      .setSortOrder("someSortField")
      .build();

```

Within your framework you can use any construction / injection needed that
calls the relevant type of storage driver for your database. If you are using
regular INTEGER auto ID columns use the DbFieldCrudStore class. Note that
different implementations will provide different feature sets based on the
builder functions but the two main both support primary and secondary indexes.

The read method on a crud store is the only 'advanced' interface, all others are
pretty much as they seem. For read, which support basic querying you can set:

* The builder ID/URN value and you will read the record for that ID.
* The value of a secondary key and all matches will be read.
* No values in the builder parameter and you will get back 'ALL' results.

This corresponds to

```java

  // read by key field
  CrudIterator<MyMessage> keyIterator = store.read(
      MyMessage.newBuilder()
      .setUrn("some-key"));

  // read secondary key
  CrudIterator<MyMessage> secondaryIterator = store.read(
      MyMessage.newBuilder()
      .setSecondary("secondary-key"));

  // read all
  CrudIterator<MyMessage> all = store.read(MyMessage.newBuilder());

```

Note that create in each driver type will set the ID or urn field for you based
on satisfying database constraints.

Note that you must set the ID / URN on update.

Note that this project was a coding exercise to avoid writing legacy SQL and
has had limited testing in large scale deployments. Before using in a production
environment consider implementing unit tests and system tests.

Build requirements dependency on protobuf 2.6.1
------------------
As most production environments have now moved to using Proto 3 this library now
requires a custom install of protobuf-lib 2.6.1 compiler to be compatible.

See https://github.com/protocolbuffers/protobuf/releases/tag/v2.6.1 for the
source bundle.

See https://github.com/protocolbuffers/protobuf/blob/master/src/README.md for
build and install instructions.


Change Log
----------

6.0.0
* Restructure the protostore changes to separate out storage engines.

5.*
* Migrated to proto3 however this introduced a number of issues with 'hasField'
  support. While 3+ protobuf supports 'has' field semantics on builders it is
  not backward compatible and actually tests for the default value which causes
  issues with values like 0 indexed sort keys and vector clock versioning.

4.7.0
* Updated dependencies.

4.3.1
* Changed to using protobuf 2.5 and added a number of features.

2.9.0
* Added support for long auto incremented keys to the field crud store. This
  should work transparently using the message id field polymorphism.

Logging
-------

Logging is done via the slf4j project. If you wish to collect log output please
configure your relevant slf4j adapter.

