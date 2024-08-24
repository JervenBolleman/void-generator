package swiss.sib.swissprot.voidcounter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

public class CountDistinctIriObjectsForAllGraphsTest {

	private Repository repository;

	@Before
	public void setup() throws IOException {
		repository = new SailRepository(new MemoryStore());
	}

	@After
	public void shutdown() {
		repository.shutDown();
	}

	@Test
	public void testEmpty() throws IOException {

		Lock writeLock = new ReentrantLock();
		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger scheduledQueries = new AtomicInteger(0);
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final CountDistinctIriObjectsForAllGraphsAtOnce countDistinctIriObjectsForAllGraphs = new CountDistinctIriObjectsForAllGraphsAtOnce(
				sd, repository, (s) -> {
				}, writeLock, new Semaphore(1), scheduledQueries, finishedQueries);
		countDistinctIriObjectsForAllGraphs.call();
		assertEquals(0, sd.getDistinctIriObjectCount());
		assertEquals(1, scheduledQueries.get());
		assertEquals(1, finishedQueries.get());
	}

	@Test
	public void testOne() throws IOException {

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.FIRST, RDF.FIRST);
			connection.add(stat, RDF.BAG);
			connection.commit();
		}
		final ServiceDescription sd = new ServiceDescription();
		GraphDescription bag = new GraphDescription();
		bag.setGraphName(RDF.BAG.stringValue());
		sd.putGraphDescription(bag);
		Lock writeLock = new ReentrantLock();
		AtomicInteger scheduledQueries = new AtomicInteger(0);
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final CountDistinctIriObjectsForAllGraphsAtOnce countDistinctIriObjectsForAllGraphs = new CountDistinctIriObjectsForAllGraphsAtOnce(
				sd, repository, (s) -> {
				}, writeLock, new Semaphore(1), scheduledQueries, finishedQueries);
		countDistinctIriObjectsForAllGraphs.call();
		assertEquals(1, sd.getDistinctIriObjectCount());
		assertEquals(1, scheduledQueries.get());
		assertEquals(1, finishedQueries.get());
	}
}
