package net.sitemorph.protostore;

import net.sitemorph.protostore.ram.*;
import net.sitemorph.queue.Tasks.*;
import org.testng.annotations.*;
import org.testng.collections.*;

import java.util.*;

import static org.testng.Assert.*;

public class InMemoryStoreTest {

  private static final String TEST_PATH = "/path",
    HOME_PATH = "/";

  @Test
  public void testReadAll() throws CrudException {
    CrudStore<Task> store = buildStore();
    store.create(Task.newBuilder()
        .setPath(TEST_PATH)
        .setRunTime(0));
    store.create(Task.newBuilder()
        .setPath(TEST_PATH)
        .setRunTime(1));
    // assume will read all
    CrudIterator<Task> tasks = store.read(Task.newBuilder());
    assertTrue(tasks.hasNext(), "Expected a task");
    assertEquals(tasks.next().getRunTime(), 0, "Expected epoch tasks");
    assertTrue(tasks.hasNext(), "Expected a second");
    assertEquals(tasks.next().getRunTime(), 1, "Expected just after epoch");
    assertFalse(tasks.hasNext(), "Should be out of tasks");
  }

  @Test
  public void testDefaultValueIndexIgnored() throws CrudException {
    CrudStore<Task> store = buildStore();
    store.create(Task.newBuilder()
      .setPath("")
      .setRunTime(0));
    store.create(Task.newBuilder()
      .setPath(TEST_PATH)
      .setRunTime(1));
    CrudIterator<Task> tasks = store.read(Task.newBuilder().setRunTime(0));
    int count = 0;
    while (tasks.hasNext()) {
      count++;
      tasks.next();
    }
    assertEquals(count, 2, "Expected just two task with zero default field");
  }

  @Test
  public void testSecondaryIndex() throws CrudException {
    CrudStore<Task> store = buildStore();
    store.create(Task.newBuilder()
      .setPath(HOME_PATH)
      .setRunTime(1));
    store.create(Task.newBuilder()
      .setPath(HOME_PATH)
      .setRunTime(2));
    store.create(Task.newBuilder()
      .setPath(TEST_PATH)
      .setRunTime(1));
    CrudIterator<Task> tasks = store.read(Task.newBuilder()
      .setPath(HOME_PATH));
    int count = 0;
    while(tasks.hasNext()) {
      count++;
      assertEquals(tasks.next().getPath(), HOME_PATH);
    }
    assertEquals(count, 2, "Expected two home paths");
  }

  @Test(expectedExceptions = MessageVectorException.class)
  public void testVectorClockError() throws CrudException {
    CrudStore<Task> store = buildStore();
    Task prior = store.create(Task.newBuilder()
      .setRunTime(0)
      .setPath(TEST_PATH));
    assertEquals(prior.getVector(), 0);
    Task update = store.update(prior.toBuilder()
      .setRunTime(7));
    assertEquals(update.getVector(), 1);
    store.update(prior.toBuilder()
    .setRunTime(11));
  }

  @Test
  public void testSortedResults() throws CrudException {
    CrudStore<Task> store = buildStore();
    store.create(Task.newBuilder()
      .setPath(TEST_PATH)
      .setRunTime(11));
    store.create(Task.newBuilder()
      .setRunTime(1)
      .setPath(TEST_PATH));
    store.create(Task.newBuilder()
      .setRunTime(7)
      .setPath(HOME_PATH));
    CrudIterator<Task> tasks = store.read(Task.newBuilder());
    List<Long> expected = Lists.newArrayList(1L, 7L, 11L);

    for (Long expect: expected) {
      assertEquals(Long.valueOf(tasks.next()
        .getRunTime()), expect);
    }
    assertFalse(tasks.hasNext());
  }

  @Test
  public void testStreamItems() throws CrudException {
    CrudStore<Task> store = buildStore();
    store.create(Task.newBuilder().setPath(TEST_PATH).setRunTime(0));
    store.create(Task.newBuilder().setPath(TEST_PATH).setRunTime(0));
    store.create(Task.newBuilder().setPath(TEST_PATH).setRunTime(0));

    long count = store.stream(Task.newBuilder())
        .count();
    assertEquals(count, 3);
  }

  private CrudStore<Task> buildStore() {
    return new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
  }
}
