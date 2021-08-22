package net.sitemorph.protostore;

import net.sitemorph.protostore.ram.InMemoryStore;
import net.sitemorph.queue.Tasks.Task;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class InMemoryStoreTest {

  private static final String TEST_PATH = "/path",
    HOME_PATH = "/";

  @Test
  public void testReadAll() throws CrudException {
    CrudStore<Task> store = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
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
  public void testSecondaryIndex() throws CrudException {
    CrudStore<Task> store = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
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
}
