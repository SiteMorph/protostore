import net.sitemorph.protostore.PreloadUrnCrudStore;
import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Tasks.Task;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Tests for the cache crud store which uses an in memory store for reads.
 *
 */
public class CacheCrudStoreTest {

  @Test
  public void testAllLoaded() throws CrudException {
    CrudStore<Task> memoryStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .build();
    memoryStore.create(Task.newBuilder()
        .setPath("/hello")
        .setRunTime(System.currentTimeMillis()));
    CrudStore<Task> cacheStore = new PreloadUrnCrudStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .setVectorField("vector")
        .setWriteStore(memoryStore)
        .build();
    CrudIterator<Task> tasks = cacheStore.read(Task.newBuilder());
    assertTrue(tasks.hasNext(), "tasks should have a next item");
  }
}
