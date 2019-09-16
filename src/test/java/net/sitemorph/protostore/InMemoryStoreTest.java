package net.sitemorph.protostore;

import net.sitemorph.protostore.ram.InMemoryStore;
import net.sitemorph.queue.Tasks.Task;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class InMemoryStoreTest {

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
        .setPath("/path")
        .setRunTime(0));
    store.create(Task.newBuilder()
        .setPath("/path")
        .setRunTime(1));
    // assume will read all
    CrudIterator<Task> tasks = store.read(Task.newBuilder());
    assertTrue(tasks.hasNext(), "Expected a task");
    assertEquals(tasks.next().getRunTime(), 0, "Expected epoch tasks");
    assertTrue(tasks.hasNext(), "Expected a second");
    assertEquals(tasks.next().getRunTime(), 1, "Expected just after epoch");
    assertFalse(tasks.hasNext(), "Should be out of tasks");
  }
}
