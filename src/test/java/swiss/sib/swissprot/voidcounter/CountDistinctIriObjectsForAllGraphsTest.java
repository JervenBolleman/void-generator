package swiss.sib.swissprot.voidcounter;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;

class CountDistinctIriObjectsForAllGraphsTest {

	private Repository repository;

	@BeforeEach
	void setup() throws IOException {
		repository = new SailRepository(new MemoryStore());
	}

	@AfterEach
	void shutdown() {
		repository.shutDown();
	}

	@Test
	void empty() throws IOException {

		Lock writeLock = new ReentrantLock();
		final ServiceDescription sd = new ServiceDescription();
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final CountDistinctIriObjectsForAllGraphsAtOnce countDistinctIriObjectsForAllGraphs = new CountDistinctIriObjectsForAllGraphsAtOnce(
				sd, repository, (s) -> {
				}, writeLock, new Semaphore(1), finishedQueries);
		countDistinctIriObjectsForAllGraphs.call();
		assertEquals(0, sd.getDistinctIriObjectCount());
		assertEquals(1, finishedQueries.get());
	}

	@Test
	void one() throws IOException {

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
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final CountDistinctIriObjectsForAllGraphsAtOnce countDistinctIriObjectsForAllGraphs = new CountDistinctIriObjectsForAllGraphsAtOnce(
				sd, repository, (s) -> {
				}, writeLock, new Semaphore(1), finishedQueries);
		countDistinctIriObjectsForAllGraphs.call();
		assertEquals(1, sd.getDistinctIriObjectCount());
		assertEquals(1, finishedQueries.get());
	}
}
