
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.PreloadUrnCrudStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Tasks.Task;

import org.testng.annotations.Test;


/**
 * Tests for the cache crud store which uses an in memory store for reads.
 *
 */
public class PreloadUrnCrudStoreTest {

  @Test
  public void testAllLoaded() throws CrudException {
    CrudStore<Task> memoryStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
    Task expect = memoryStore.create(Task.newBuilder()
        .setPath("/hello")
        .setRunTime(System.currentTimeMillis()));
    CrudStore<Task> cacheStore = new PreloadUrnCrudStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setWriteStore(memoryStore)
        .build();
    CrudIterator<Task> tasks = cacheStore.read(Task.newBuilder());
    assertTrue(tasks.hasNext(), "tasks should have a next item");
    assertEquals(expect, tasks.next(), "Expected the generated test");
    assertFalse(tasks.hasNext(), "Didn't expect any more tasks");
  }

  @Test
  public void testSecondaryIndex() throws CrudException {
    CrudStore<Task> memoryStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
    Task expect = memoryStore.create(Task.newBuilder()
        .setPath("/home")
        .setRunTime(System.currentTimeMillis()));
    Task other = memoryStore.create(Task.newBuilder()
        .setPath("/away")
        .setRunTime(System.currentTimeMillis()));
    CrudStore<Task> cacheStore = new PreloadUrnCrudStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setWriteStore(memoryStore)
        .build();
    CrudIterator<Task> tasks = cacheStore.read(Task.newBuilder()
        .setPath("/home"));
    assertTrue(tasks.hasNext(), "Expected to have a result");
    assertEquals(expect, tasks.next(), "Expected home task");
    assertFalse(tasks.hasNext(), "Didn't expect the other tasks");

  }
}
