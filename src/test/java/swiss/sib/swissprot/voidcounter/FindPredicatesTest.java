package swiss.sib.swissprot.voidcounter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import swiss.sib.swissprot.servicedescription.GraphDescription;
import swiss.sib.swissprot.servicedescription.ServiceDescription;
import swiss.sib.swissprot.voidcounter.FindPredicates;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FindPredicatesTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(FindPredicatesTest.class);

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private Repository repository;

	@Before
	public void setup() throws IOException {
		LOGGER.debug("running setup");
		repository = new SailRepository(new MemoryStore());
		repository.init();
	}

	@After
	public void shutdown() {
		LOGGER.debug("running shutdown");
		repository.shutDown();
		LOGGER.debug("ran shutdown");
	}

	@Test
	public void testEmpty() throws IOException {

		final ServiceDescription sd = new ServiceDescription();
		final GraphDescription gd = new GraphDescription();
		gd.setGraphName("https://sparql.uniprot.org/uniprot");
		sd.getGraphs().add(gd);
		Lock writeLock = new ReentrantLock();
		List<Future<Exception>> futures = new ArrayList<>();

		AtomicInteger scheduledQueries = new AtomicInteger(0);
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				futures, Executors.newSingleThreadExecutor(), writeLock, new Semaphore(1), scheduledQueries,
				finishedQueries, (isd) -> {
				}, sd);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(0, gd.getPredicates().size());
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
		final GraphDescription gd = new GraphDescription();
		gd.setGraphName(RDF.BAG.stringValue());
		sd.getGraphs().add(gd);
		Lock writeLock = new ReentrantLock();
		List<Future<Exception>> futures = new ArrayList<>();
		AtomicInteger scheduledQueries = new AtomicInteger(0);
		AtomicInteger finishedQueries = new AtomicInteger(0);

		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				futures, Executors.newSingleThreadExecutor(), writeLock, new Semaphore(1), scheduledQueries,
				finishedQueries, (isd) -> {
				}, sd);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(1, gd.getPredicates().size());
		assertEquals(1, scheduledQueries.get());
		assertEquals(1, finishedQueries.get());
	}

	@Test
	public void testOneLast() throws IOException {

		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			final SimpleValueFactory svf = SimpleValueFactory.getInstance();
			Statement stat = svf.createStatement(RDF.FIRST, RDF.FIRST, RDF.FIRST);
			connection.add(stat, RDF.BAG);
			connection.commit();
		}
		final ServiceDescription sd = new ServiceDescription();
		final GraphDescription gd = new GraphDescription();
		gd.setGraphName(RDF.BAG.stringValue());
		sd.getGraphs().add(gd);
		Lock writeLock = new ReentrantLock();
		List<Future<Exception>> futures = new ArrayList<>();
		AtomicInteger scheduledQueries = new AtomicInteger(0);
		AtomicInteger finishedQueries = new AtomicInteger(0);
		final FindPredicates countDistinctIriSubjectsForAllGraphs = new FindPredicates(gd, repository, Set.of(),
				futures, Executors.newSingleThreadExecutor(), writeLock, new Semaphore(1), scheduledQueries,
				finishedQueries, (isd) -> {
				}, sd);
		countDistinctIriSubjectsForAllGraphs.call();
		assertEquals(1, gd.getPredicates().size());
		assertEquals(1, scheduledQueries.get());
		assertEquals(1, finishedQueries.get());
	}
}
